"""Application orchestrator — runs bot and executor concurrently."""

import asyncio
import logging
import math
import signal
import ssl
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime
from html import escape as html_escape
from zoneinfo import ZoneInfo

import docker
from defusedxml import ElementTree as DefusedET

from .bot import ConfirmationBot
from .config import Config
from .db import Database
from .executor import ExecutionResult, TradeExecutor
from .models import TradeSignal
from .safety import check_duplicate_signal, check_position_limits, is_market_open, time_until_market_open
from .webhook import WebhookServer

logger = logging.getLogger(__name__)

FLEX_REQUEST_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
FLEX_STATEMENT_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement"


class App:
    """Owns all components and runs them via asyncio.gather."""

    def __init__(self, config: Config):
        self.config = config
        self.db = Database(config.db_path)
        self.executor = TradeExecutor(config, on_disconnect=self._on_gateway_status, on_fill=self._on_order_event)

        # Preserve original config baselines (before Flex modifies net_deposits)
        self._deposit_baselines = {
            a.name: a.net_deposits for a in config.accounts
        }

        # Execution lock — prevents concurrent trade confirmations
        self._execution_lock = asyncio.Lock()

        # Gateway pause/resume state — derive container names from config
        self._gateway_containers = [a.gateway_host for a in config.accounts]
        self._gateway_paused = False
        self._resume_task: asyncio.Task | None = None

        # Webhook API (optional — started only when webhook_secret is set)
        self.webhook: WebhookServer | None = None
        if config.webhook_secret:
            self.webhook = WebhookServer(
                secret=config.webhook_secret,
                port=config.webhook_port,
                on_signal=self._on_webhook_signal,
            )

        # Wire all callbacks into the bot
        self.bot = ConfirmationBot(
            bot_token=config.bot_token,
            admin_chat_id=config.admin_chat_id,
            on_confirm=self._on_trade_confirmed,
            on_positions=self._on_positions_requested,
            on_kill=self._on_kill_requested,
            on_portfolio=self._on_portfolio_requested,
            on_account=self._on_account_requested,
            on_signals=self._on_signals_requested,
            on_health=self._on_health_requested,
            on_deposits=self._on_deposits_requested,
            on_value=self._on_value_requested,
            on_buy_preview=self._on_buy_preview,
            on_buy_execute=self._on_buy_execute,
            on_sell_preview=self._on_sell_preview,
            on_sell_execute=self._on_sell_execute,
            on_list_positions=self._on_list_positions,
            on_option_expiries=self._on_option_expiries,
            on_option_strikes=self._on_option_strikes,
            on_info=self._on_info_requested,
            on_pause=self._on_pause_requested,
            on_resume=self._on_resume_requested,
            on_orders=self._on_orders_requested,
            on_trades=self._on_trades_requested,
            on_cancel_order=self._on_cancel_order,
            on_cancel_all=self._on_cancel_all_orders,
            on_price=self._on_price_requested,
        )

    async def run(self) -> None:
        """Initialize all components and run concurrently."""
        await self.db.init()

        if self.config.accounts:
            await self.executor.connect_all()
            for connector in self.executor.connectors.values():
                await connector.subscribe_pnl()
            await self._sync_flex_deposits()
            await self._sync_positions()

        if self.webhook:
            await self.webhook.start()

        tasks = []
        if self.config.bot_token:
            tasks.append(self.bot.start())
        if self.config.accounts:
            tasks.append(self._periodic_sync())

        if not tasks:
            if self.webhook:
                logger.info("Running webhook API only (no bot token or accounts configured)")
                # Keep alive — webhook server runs in its own aiohttp runner
                stop_event = asyncio.Event()
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(sig, stop_event.set)
                try:
                    await stop_event.wait()
                finally:
                    for sig in (signal.SIGTERM, signal.SIGINT):
                        loop.remove_signal_handler(sig)
                    await self.shutdown()
                return
            else:
                logger.error("Nothing to run: no bot token, no accounts, no webhook")
                await self.shutdown()
                return

        logger.info("Starting %d concurrent tasks...", len(tasks))
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Graceful shutdown of all components."""
        logger.info("Shutting down...")
        if self.webhook:
            await self.webhook.stop()
        if self.config.bot_token:
            await self.bot.stop()
        await self.executor.disconnect_all()
        await self.db.close()
        logger.info("Shutdown complete")

    async def _on_webhook_signal(self, signal: TradeSignal) -> dict:
        """Handle an incoming webhook signal — save to DB and send for confirmation."""
        if await check_duplicate_signal(self.db, signal.ticker, signal.action):
            logger.info("Duplicate webhook signal skipped: %s %s", signal.action, signal.ticker)
            return {"status": "duplicate_skipped", "ticker": signal.ticker, "action": signal.action}

        signal_id = await self.db.save_signal(
            message_id=signal.message_id or 0,
            ticker=signal.ticker,
            action=signal.action,
            target_weight_pct=signal.target_weight_pct,
            amount_description=signal.amount_description,
            related_ticker=signal.related_ticker,
            raw_text=signal.raw_text,
            source=signal.source,
        )
        await self.db.log_audit(
            "signal_received", signal_id, signal.ticker,
            f"Webhook {signal.action} ${signal.ticker}",
        )
        if self.config.bot_token:
            await self.bot.send_confirmation(signal, signal_id=signal_id)
            return {"signal_id": signal_id, "status": "pending_confirmation"}
        logger.warning("Signal %d saved but no bot configured to confirm it", signal_id)
        return {"signal_id": signal_id, "status": "saved_no_bot"}

    async def _on_trade_confirmed(
        self, signal_id: int, signal: TradeSignal
    ) -> list[ExecutionResult]:
        """Called when admin confirms a trade via the bot."""
        logger.info(f"Trade confirmed: {signal.action} ${signal.ticker} (signal_id={signal_id})")
        await self.db.log_audit("trade_confirmed", signal_id, signal.ticker,
                                f"{signal.action} confirmed by admin")

        async with self._execution_lock:
            # Market hours check
            if not is_market_open():
                wait = time_until_market_open()
                msg = f"Market closed — next open in {wait}"
                logger.warning(f"[{signal.ticker}] {msg}")
                await self.db.update_signal_status(signal_id, "skipped")
                await self.db.log_audit("market_closed", signal_id, signal.ticker, msg)
                return [ExecutionResult(
                    account_name="all",
                    success=False,
                    error=msg,
                )]

            # Duplicate detection
            if await check_duplicate_signal(self.db, signal.ticker, signal.action):
                await self.db.update_signal_status(signal_id, "skipped")
                await self.db.log_audit("duplicate_skipped", signal_id, signal.ticker,
                                        f"Duplicate {signal.action} within window")
                return [ExecutionResult(
                    account_name="all",
                    success=False,
                    error=f"Duplicate signal: {signal.action} ${signal.ticker} already processed recently",
                )]

            # Position limit checks (per account) — skip breaching accounts
            blocked_accounts: set[str] = set()
            for account_cfg in self.config.accounts:
                positions = await self.db.get_positions(account_cfg.name)
                limit_err = check_position_limits(
                    signal.action, signal.ticker,
                    signal.target_weight_pct or 5.0,
                    account_cfg.max_position_pct,
                    account_cfg.max_allocation_pct,
                    positions,
                )
                if limit_err:
                    blocked_accounts.add(account_cfg.name)
                    logger.warning(f"[{account_cfg.name}] Position limit: {limit_err}")
                    await self.db.log_audit("limit_breach", signal_id, signal.ticker,
                                            f"{account_cfg.name}: {limit_err}")

            # Update signal status in DB
            await self.db.update_signal_status(signal_id, "confirmed")

            # Execute across all connected accounts (skip limit-breaching ones)
            results = await self.executor.execute(signal, exclude_accounts=blocked_accounts)

            # Save execution results to DB (status="submitted" — actual fills update later)
            for result in results:
                status = "submitted" if result.success else "failed"
                await self.db.save_execution(
                    signal_id=signal_id,
                    account_name=result.account_name,
                    order_id=result.order_id,
                    filled_qty=0,
                    avg_price=0.0,
                    target_pct=signal.target_weight_pct or 0,
                    actual_pct=0.0,
                    status=status,
                    error=result.error,
                )

            # Update signal status based on results
            any_success = any(r.success for r in results)
            final_status = "executed" if any_success else "failed"
            await self.db.update_signal_status(signal_id, final_status)
            await self.db.log_audit(
                f"execution_{final_status}", signal_id, signal.ticker,
                "; ".join(
                    f"{r.account_name}: {'OK' if r.success else r.error}"
                    for r in results
                ),
            )

            return results

    async def _on_positions_requested(self) -> str:
        """Called when admin requests /positions."""
        if not self.executor.connectors:
            return "No IBKR accounts connected"

        lines = []
        for name, connector in self.executor.connectors.items():
            nlv = await connector.get_nlv()
            positions = await connector.get_positions()
            lines.append(f"<b>{name}</b> (NLV: ${nlv:,.0f})")
            if positions:
                for pos in positions:
                    symbol = pos.contract.symbol if hasattr(pos, 'contract') else str(pos)
                    qty = pos.position if hasattr(pos, 'position') else 0
                    lines.append(f"  {symbol}: {qty}")
            else:
                lines.append("  No positions")
            lines.append("")

        return "\n".join(lines) if lines else "No data"

    async def _on_kill_requested(self) -> str:
        """Called when admin confirms /kill — cancel all open orders."""
        if not self.executor.connectors:
            return "No IBKR accounts connected"

        results = []
        for name, connector in self.executor.connectors.items():
            try:
                cancelled = await connector.cancel_all_orders()
                results.append(f"{name}: cancelled {cancelled} orders")
            except Exception as e:
                results.append(f"{name}: ERROR \u2014 {e}")

        return "\n".join(results)

    async def _on_portfolio_requested(self) -> str:
        """Called when admin requests /portfolio — show returns per account."""
        if not self.executor.connectors:
            return "No IBKR accounts connected"

        lines = ["\U0001f4bc <b>Portfolio Summary</b>\n"]
        total_nlv = 0.0
        total_deposits = 0.0

        for account_cfg in self.config.accounts:
            name = account_cfg.name
            display = account_cfg.display_name or name
            connector = self.executor.connectors.get(name)
            if not connector:
                lines.append(f"\u26a0\ufe0f <b>{display}</b>: disconnected")
                continue

            nlv_all = await connector.get_nlv_by_currency()
            nlv_eur = nlv_all.get("EUR", 0.0)
            deposits = account_cfg.net_deposits
            total_return = nlv_eur - deposits
            return_pct = (total_return / deposits * 100) if deposits > 0 else 0
            total_nlv += nlv_eur
            total_deposits += deposits

            arrow = "\u2705" if total_return >= 0 else "\U0001f534"
            lines.append(
                f"{arrow} <b>{display}</b>\n"
                f"   NLV: \u20ac{nlv_eur:,.0f}\n"
                f"   Net Deposited: \u20ac{deposits:,.0f}\n"
                f"   Return: \u20ac{total_return:+,.0f} ({return_pct:+.1f}%)"
            )

        # Totals
        grand_return = total_nlv - total_deposits
        grand_pct = (grand_return / total_deposits * 100) if total_deposits > 0 else 0
        lines.append(
            f"\n\U0001f3af <b>Combined</b>\n"
            f"   NLV: \u20ac{total_nlv:,.0f}\n"
            f"   Net Deposited: \u20ac{total_deposits:,.0f}\n"
            f"   Return: \u20ac{grand_return:+,.0f} ({grand_pct:+.1f}%)"
        )
        return "\n".join(lines)

    async def _on_value_requested(self, mode: str = "total") -> str:
        """Called when admin requests /v — compact portfolio snapshot."""
        if not self.executor.connectors:
            return "\u26a0\ufe0f No IBKR accounts connected"

        now = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%d %b %H:%M")
        lines = [f"<b>\U0001f4ca Portfolio</b> \u2014 {now}\n"]

        total_nlv = 0.0
        total_deposits = 0.0
        total_daily = 0.0
        all_positions = []  # (symbol, qty, pnl, market_value, avg_cost, mkt_price, conId, account)
        daily_by_conid = {}  # conId -> daily P&L

        for i, account_cfg in enumerate(self.config.accounts):
            name = account_cfg.name
            display = account_cfg.display_name or name
            connector = self.executor.connectors.get(name)
            if not connector:
                prefix = "\u250c" if i == 0 else "\u2514"
                lines.append(f"{prefix} <b>{display}</b>  \u26a0\ufe0f disconnected")
                continue

            nlv_all = await connector.get_nlv_by_currency()
            nlv_eur = nlv_all.get("EUR", 0.0)
            deposits = account_cfg.net_deposits
            ret = nlv_eur - deposits
            ret_pct = (ret / deposits * 100) if deposits > 0 else 0
            total_nlv += nlv_eur
            total_deposits += deposits

            # Get daily P&L for account (IBKR returns in account base currency)
            daily_data = await connector.get_daily_pnl()
            acct_daily = daily_data.get("dailyPnL", 0.0)

            # Get per-position daily P&L (sum across accounts, don't overwrite)
            pos_daily = await connector.get_positions_daily_pnl()
            for cid, daily_val in pos_daily.items():
                daily_by_conid[cid] = daily_by_conid.get(cid, 0.0) + daily_val

            arrow = "\u25b2" if ret >= 0 else "\u25bc"
            day_arrow = "\u25b2" if acct_daily >= 0 else "\u25bc"
            prefix = "\u250c" if i == 0 else "\u2514"
            # dailyPnL is in account base currency (EUR for EUR accounts)
            # For EUR accounts: already EUR, no conversion needed
            # For USD accounts: convert to EUR
            base_currency = next(iter(nlv_all.keys()), "EUR")
            usd_rate = await connector.get_exchange_rate("USD")
            if base_currency == "EUR":
                acct_daily_eur = acct_daily
            else:
                acct_daily_eur = acct_daily * usd_rate
            total_daily += acct_daily_eur

            ret_str = f"  {arrow} {ret_pct:+.1f}%" if deposits > 0 else ""
            lines.append(
                f"{prefix} <b>{display}</b>  <b>\u20ac{nlv_eur:,.0f}</b>"
                f"{ret_str}  "
                f"<u>{day_arrow} \u20ac{acct_daily_eur:+,.0f} today</u>"
            )

            portfolio = await connector.get_portfolio()
            for item in portfolio:
                all_positions.append((
                    item.contract.symbol,
                    int(item.position),
                    item.unrealizedPNL,
                    item.marketValue,
                    item.averageCost,
                    item.marketPrice,
                    item.contract.conId,
                    name,
                    item.contract,
                ))

        # Combined totals
        grand_ret = total_nlv - total_deposits
        grand_pct = (grand_ret / total_deposits * 100) if total_deposits > 0 else 0
        arrow = "\u25b2" if grand_ret >= 0 else "\u25bc"
        # total_daily is already summed in EUR (converted per-account above)
        total_daily_eur = total_daily
        # Get USD rate for position-level conversions below
        usd_rate = 1.0
        for cfg in self.config.accounts:
            c = self.executor.connectors.get(cfg.name)
            if c:
                usd_rate = await c.get_exchange_rate("USD")
                break
        day_icon = "\U0001f7e2" if total_daily_eur >= 0 else "\U0001f534"

        lines.append("\u2500" * 18)
        ret_part = f" ({grand_pct:+.1f}%)" if total_deposits > 0 else ""
        lines.append(
            f"\U0001f4b0 <b>\u20ac{total_nlv:,.0f}</b>  "
            f"{arrow} \u20ac{grand_ret:+,.0f}{ret_part}"
        )
        lines.append(
            f"{day_icon} <b>Today:</b> \u20ac{total_daily_eur:+,.0f}"
        )
        lines.append(f"   Deposited \u20ac{total_deposits:,.0f}\n")

        # Aggregate positions by symbol across accounts — keep per-account detail
        agg = defaultdict(lambda: {
            "qty": 0, "pnl": 0.0, "value": 0.0, "cost": 0.0,
            "daily": 0.0, "mkt_price": 0.0, "accounts": {}, "contract": None,
        })
        daily_counted = set()  # avoid double-counting (daily_by_conid already sums accounts)
        for sym, qty, pnl, val, avg_cost, mkt_price, con_id, acct, contract_obj in all_positions:
            entry = agg[sym]
            entry["qty"] += qty
            entry["pnl"] += pnl
            entry["value"] += val
            entry["cost"] += avg_cost * qty
            if con_id not in daily_counted:
                entry["daily"] += daily_by_conid.get(con_id, 0.0)
                daily_counted.add(con_id)
            if not entry["mkt_price"] and mkt_price > 0:
                entry["mkt_price"] = mkt_price
            # Per-account qty
            acct_display = next(
                (c.display_name[0] for c in self.config.accounts
                 if c.name == acct and c.display_name), acct[0].upper()
            )
            entry["accounts"][acct_display] = entry["accounts"].get(acct_display, 0) + qty
            # Attach contract from first pass (no second get_portfolio() needed)
            if not entry["contract"] and contract_obj:
                entry["contract"] = contract_obj

        # Fetch underlying stock prices + option data in batch
        unique_symbols = list(agg.keys())
        stock_prices: dict[str, dict[str, float]] = {}
        option_data: dict[int, dict[str, float]] = {}
        first_connector = next(
            (self.executor.connectors.get(cfg.name)
             for cfg in self.config.accounts
             if cfg.name in self.executor.connectors
             and self.executor.connectors[cfg.name].is_connected),
            None,
        )
        if first_connector:
            if unique_symbols:
                try:
                    stock_prices = await asyncio.wait_for(
                        first_connector.get_stock_prices_batch(unique_symbols),
                        timeout=15,
                    )
                except Exception as e:
                    logger.warning("Stock prices failed: %s", e)
            opt_contracts = [
                d["contract"] for d in agg.values()
                if d["contract"] and hasattr(d["contract"], "strike") and d["contract"].strike
            ]
            if opt_contracts:
                try:
                    option_data = await asyncio.wait_for(
                        first_connector.get_option_data_batch(opt_contracts),
                        timeout=15,
                    )
                except Exception as e:
                    logger.warning("Option data failed: %s", e)

        sorted_positions = sorted(agg.items(), key=lambda x: abs(x[1]["value"]), reverse=True)

        if not sorted_positions:
            lines.append("<i>No positions</i>")
            return "\n".join(lines)

        lines.append("\n\u2500\u2500 <b>Positions</b> \u2500\u2500")

        for sym, data in sorted_positions:
            qty = data["qty"]
            pnl_eur = data["pnl"] * usd_rate
            daily_eur = data["daily"] * usd_rate
            value_eur = data["value"] * usd_rate
            weight = (abs(value_eur) / total_nlv * 100) if total_nlv > 0 else 0
            pnl_pct = (data["pnl"] / abs(data["cost"]) * 100) if abs(data["cost"]) > 0 else 0

            # Color dot — reflects daily change in day mode, total P&L otherwise
            prev_val = abs(data["value"]) - data["daily"]
            daily_val_pct = (data["daily"] / prev_val * 100) if prev_val > 0 else 0.0
            dot_pct = daily_val_pct if mode == "day" else pnl_pct
            if dot_pct > 2:
                dot = "\U0001f7e2"
            elif dot_pct < -2:
                dot = "\U0001f534"
            else:
                dot = "\U0001f7e1"

            # Contract detail (strike/exp/right)
            c = data["contract"]
            detail_str = ""
            if c and hasattr(c, "strike") and c.strike:
                exp = c.lastTradeDateOrContractMonth or ""
                exp_short = f"{exp[4:6]}/{exp[2:4]}" if len(exp) >= 6 else exp
                detail_str = f" {c.strike}{c.right} {exp_short}"

            # Per-account qty split
            acct_parts = "/".join(f"{k}:{v}" for k, v in sorted(data["accounts"].items()))

            lines.append(
                f"\n{dot} <code>{sym}</code>{detail_str}  \u2014  <b>{weight:.0f}%</b>"
            )

            # Detail block inside blockquote for visual grouping
            bq = []

            # Stock price with day range (hide range when no spread, e.g. pre-market)
            stk = stock_prices.get(sym)
            if stk:
                stk_line = f"Stk ${stk['price']:.2f}"
                if stk['high'] != stk['low']:
                    stk_line += f"  (L ${stk['low']:.2f} \u2013 H ${stk['high']:.2f})"
                bq.append(stk_line)

            # Option price with day range (hide range when no spread)
            con_id = c.conId if c else None
            opt_info = option_data.get(con_id) if con_id else None
            opt_price = data["mkt_price"]
            if opt_price > 0:
                opt_daily = data["daily"] / (qty * 100) if qty > 0 else 0.0
                if opt_daily >= 0:
                    chg = f"\u25b2${opt_daily:.2f}"
                else:
                    chg = f"\u25bc${abs(opt_daily):.2f}"
                opt_line = f"Opt ${opt_price:.2f} {chg}"
                if opt_info and opt_info['high'] != opt_info['low']:
                    opt_line += (
                        f"\nL ${opt_info['low']:.2f} \u2013 H ${opt_info['high']:.2f}"
                    )
                bq.append(opt_line)

            bq.append(f"{qty}x ({acct_parts}) \u2022 \u20ac{value_eur:,.0f}")

            # P&L line
            pnl_arrow = "\u25b2" if pnl_eur >= 0 else "\u25bc"
            day_arrow = "\u25b2" if daily_eur >= 0 else "\u25bc"
            total_str = f"{pnl_arrow}\u20ac{pnl_eur:+,.0f} ({pnl_pct:+.1f}%)"
            day_str = f"{day_arrow}\u20ac{daily_eur:+,.0f} ({daily_val_pct:+.1f}%)"
            if mode == "day":
                bq.append(f"<b>Today: {day_str}</b>")
            else:
                bq.append(f"<b>P&L: {total_str}</b>")

            lines.append("<blockquote>" + "\n".join(bq) + "</blockquote>")

        # Cash weight (use EUR values for consistency with EUR NLV)
        positions_weight = sum(
            (abs(d["value"] * usd_rate) / total_nlv * 100) if total_nlv > 0 else 0
            for _, d in sorted_positions
        )
        cash_weight = max(0, 100 - positions_weight)
        if cash_weight > 0.5:
            # Aggregate cash balances across all accounts
            all_cash: dict[str, float] = {}
            for cfg in self.config.accounts:
                connector = self.executor.connectors.get(cfg.name)
                if not connector:
                    continue
                balances = await connector.get_cash_balances()
                for ccy, amount in balances.items():
                    all_cash[ccy] = all_cash.get(ccy, 0.0) + amount
            if all_cash:
                cash_parts = "  ".join(
                    f"{ccy} {amt:+,.0f}" for ccy, amt in sorted(all_cash.items())
                )
                lines.append(f"\n\u26aa <b>Cash:</b> {cash_weight:.0f}%  ({cash_parts})")
            else:
                lines.append(f"\n\u26aa <b>Cash:</b> {cash_weight:.0f}%")

        return "\n".join(lines)

    # ── Order Entry (Percentage-Based Trading) ────────────────────────────────
    #
    # SAFETY RULES:
    # - All sizing is by % of NLV — same % on every account
    # - Hard cap: no single position > max_position_pct from config
    # - Margin check: refuse if order cost > available cash
    # - Always requires explicit confirmation before execution

    MAX_SINGLE_ORDER_PCT = 20.0  # Hard cap: never allocate >20% in a single order
    MAX_TOTAL_POSITION_PCT = 35.0  # Hard cap: never let one ticker exceed 35% of NLV

    def _parse_order_args(self, raw: str) -> dict:
        """Parse order arguments: TICKER PCT% [expiry] [strike+right] PRICE|MKT.

        The last argument must be a limit price or 'MKT' for market orders.

        Examples:
          'IREN 5% 12.75'           — 5% of NLV, auto LEAPS, limit $12.75
          'CIFR 5% Jan28 27C 8.50'  — specific contract, limit $8.50
          'IREN all 1.60'            — sell all, limit $1.60
          'IREN half mkt'            — sell half, market order
        """
        parts = raw.strip().upper().split()
        if not parts:
            raise ValueError("No ticker specified")

        ticker = parts[0]
        qty_raw = parts[1] if len(parts) > 1 else "5"
        expiry = None
        strike = None
        right = "C"
        limit_price = None  # None=not specified, "MKT"=market, float=limit

        # Parse quantity/percentage
        if qty_raw in ("ALL", "HALF", "THIRD", "QUARTER"):
            pct = qty_raw.lower()
        elif qty_raw.endswith("%"):
            pct = float(qty_raw[:-1])
        else:
            try:
                pct = float(qty_raw)
            except ValueError:
                raise ValueError(f"Invalid percentage: {qty_raw}")

        # Extract price from last argument (MKT or number without C/P suffix)
        remaining = list(parts[2:])
        if remaining:
            last = remaining[-1]
            if last == "MKT":
                limit_price = "MKT"
                remaining = remaining[:-1]
            elif not last[-1].isalpha():
                try:
                    limit_price = float(last)
                    remaining = remaining[:-1]
                except ValueError:
                    pass

        # Parse remaining args (expiry, strike)
        for part in remaining:
            # Expiry like 'JAN28' or '20280121'
            if len(part) >= 5 and part[:3].isalpha():
                month_map = {
                    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
                    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
                }
                month = month_map.get(part[:3])
                year = "20" + part[3:] if len(part[3:]) == 2 else part[3:]
                if month and year:
                    expiry = f"{year}{month}17"
            elif len(part) == 8 and part.isdigit():
                expiry = part
            # Strike+Right like '27C' or '85P'
            elif part[-1] in ("C", "P"):
                try:
                    strike = float(part[:-1])
                    right = part[-1]
                except ValueError:
                    pass

        if limit_price is None:
            raise ValueError(
                "Specify a limit price or MKT for market order.\n"
                "Example: /buy IREN 5% 12.75  or  /sell IREN all mkt"
            )

        return {
            "ticker": ticker,
            "pct": pct,
            "expiry": expiry,
            "strike": strike,
            "right": right,
            "limit_price": limit_price,
        }

    async def _on_buy_preview(self, raw_args: str) -> tuple[str, dict]:
        """Parse buy args, look up contract, return preview with safety checks."""
        if not self.executor.connectors:
            raise ValueError("No IBKR accounts connected")

        parsed = self._parse_order_args(raw_args)
        ticker = parsed["ticker"]
        target_pct = parsed["pct"]

        if isinstance(target_pct, str):
            raise ValueError("Use a percentage for /buy (e.g., /buy IREN 5%)")
        if target_pct <= 0:
            raise ValueError("Percentage must be positive")

        # SAFETY: Hard cap on single order
        if target_pct > self.MAX_SINGLE_ORDER_PCT:
            raise ValueError(
                f"\u26a0\ufe0f Safety limit: max {self.MAX_SINGLE_ORDER_PCT}% per order. "
                f"You requested {target_pct}%."
            )

        # Use all connected accounts (same % on each)
        accounts = self._resolve_accounts("all")
        if not accounts:
            raise ValueError("No accounts connected")

        # Use first connector to look up contract
        first_connector = self.executor.connectors[accounts[0].name]

        # Find contract
        if parsed["expiry"] and parsed["strike"]:
            from ib_async import Option
            contract = Option(
                ticker, parsed["expiry"], parsed["strike"],
                parsed["right"], "SMART"
            )
            qualified = await first_connector.qualify_contracts(contract)
            if not qualified:
                raise ValueError(
                    f"Could not find {ticker} {parsed['expiry']} "
                    f"{parsed['strike']}{parsed['right']}"
                )
            contract = qualified[0]
        else:
            contract = await first_connector.find_leaps_contract(ticker, parsed["right"])

        # Get price
        option_price = await first_connector.get_option_price(contract)
        cost_per = option_price * 100  # options multiplier

        # Calculate qty per account from their individual NLV at same %
        allocations = {}
        warnings = []
        usd_rate = await first_connector.get_exchange_rate("USD")

        for acfg in accounts:
            connector = self.executor.connectors.get(acfg.name)
            nlv_all = await connector.get_nlv_by_currency()
            nlv_usd = nlv_all.get("USD", 0.0)  # Cost is in USD

            if nlv_usd <= 0:
                warnings.append(f"\u26a0\ufe0f {acfg.display_name}: no NLV")
                continue

            target_value = nlv_usd * (target_pct / 100)
            # Round up when margin is available — prefer slight over-deployment
            margin_active = acfg.margin_mode in ("soft", "hard") and acfg.is_margin_account
            if margin_active:
                qty = max(1, math.ceil(target_value / cost_per))
            else:
                qty = max(1, int(target_value / cost_per))
            actual_cost = qty * cost_per
            actual_pct = (actual_cost / nlv_usd) * 100

            # SAFETY: Check existing position for this ticker
            portfolio = await connector.get_portfolio()
            existing_value = sum(
                abs(p.marketValue) for p in portfolio
                if p.contract.symbol == ticker
            )
            existing_pct = (existing_value / nlv_usd) * 100 if nlv_usd > 0 else 0
            new_total_pct = existing_pct + actual_pct

            if new_total_pct > self.MAX_TOTAL_POSITION_PCT:
                warnings.append(
                    f"\u26a0\ufe0f {acfg.display_name}: would be {new_total_pct:.0f}% "
                    f"in {ticker} (limit {self.MAX_TOTAL_POSITION_PCT:.0f}%)"
                )
                # Reduce qty to stay within limit
                max_add_pct = max(0, self.MAX_TOTAL_POSITION_PCT - existing_pct)
                max_add_value = nlv_usd * (max_add_pct / 100)
                qty = max(0, int(max_add_value / cost_per))
                if qty == 0:
                    warnings.append(f"  \u2192 {acfg.display_name}: SKIPPED (already at limit)")
                    continue
                actual_cost = qty * cost_per
                actual_pct = (actual_cost / nlv_usd) * 100

            # SAFETY: Check available cash + margin cap
            available_funds = await connector.get_available_funds("USD")
            margin_budget = (
                acfg.max_margin_usd
                if margin_active and acfg.max_margin_usd > 0
                else 0
            )
            budget = available_funds + margin_budget
            if actual_cost > budget:
                max_affordable = int(budget / cost_per)
                if max_affordable <= 0:
                    margin_note = f" + ${acfg.max_margin_usd:,.0f} margin" if acfg.max_margin_usd > 0 else ""
                    warnings.append(
                        f"\u26a0\ufe0f {acfg.display_name}: INSUFFICIENT FUNDS "
                        f"(need ${actual_cost:,.0f}, have ${available_funds:,.0f}{margin_note})"
                    )
                    continue
                warnings.append(
                    f"\u26a0\ufe0f {acfg.display_name}: reduced {qty}\u2192{max_affordable} "
                    f"(funds: ${available_funds:,.0f}, margin: ${acfg.max_margin_usd:,.0f})"
                )
                qty = max_affordable
                actual_cost = qty * cost_per
                actual_pct = (actual_cost / nlv_usd) * 100

            allocations[acfg.name] = {
                "qty": qty,
                "cost_usd": actual_cost,
                "pct_of_nlv": actual_pct,
                "nlv_usd": nlv_usd,
            }

        if not allocations:
            raise ValueError("No viable allocations (safety limits or insufficient funds)")

        # Format preview
        exp_str = contract.lastTradeDateOrContractMonth
        exp_display = f"{exp_str[4:6]}/{exp_str[0:4]}"
        total_qty = sum(a["qty"] for a in allocations.values())
        total_cost_usd = sum(a["cost_usd"] for a in allocations.values())
        total_cost_eur = total_cost_usd * usd_rate

        limit_price = parsed["limit_price"]
        if limit_price == "MKT":
            price_label = "MARKET ORDER"
        else:
            price_label = f"Limit ${limit_price:.2f}"

        lines = [
            "\U0001f4e6 <b>BUY Order Preview</b>\n",
            f"<b>{ticker}</b> {contract.strike}{contract.right} exp {exp_display}",
            f"<code>{contract.localSymbol}</code>\n",
            f"<b>Market Price:</b> ${option_price:.2f}/contract (${cost_per:,.0f} per)",
            f"<b>Order:</b> {price_label}",
            f"<b>Target:</b> {target_pct}% of each account's NLV",
            f"<b>Total:</b> {total_qty} contracts \u2014 ${total_cost_usd:,.0f} (\u20ac{total_cost_eur:,.0f})\n",
            "<b>Per Account:</b>",
        ]
        for acfg in accounts:
            if acfg.name not in allocations:
                continue
            display = acfg.display_name or acfg.name
            a = allocations[acfg.name]
            lines.append(
                f"  {display}: {a['qty']}x \u2014 ${a['cost_usd']:,.0f} "
                f"({a['pct_of_nlv']:.1f}% of NLV)"
            )

        if warnings:
            lines.append("\n<b>Safety Warnings:</b>")
            lines.extend(warnings)

        text = "\n".join(lines)

        order_details = {
            "action": "BUY",
            "ticker": ticker,
            "contract_conId": contract.conId,
            "contract_symbol": contract.localSymbol,
            "expiry": exp_str,
            "strike": contract.strike,
            "right": contract.right,
            "option_price": option_price,
            "limit_price": limit_price,
            "allocations": {k: v["qty"] for k, v in allocations.items()},
            "target_pct": target_pct,
        }
        return text, order_details

    async def _on_buy_execute(self, order_details: dict) -> str:
        """Execute a confirmed buy order across accounts (with live safety re-check)."""
        from ib_async import LimitOrder, MarketOrder, Option

        ticker = order_details["ticker"]
        allocations = order_details["allocations"]
        option_price = order_details["option_price"]
        limit_price = order_details.get("limit_price", "MKT")

        results = []
        for account_name, qty in allocations.items():
            if qty <= 0:
                continue
            connector = self.executor.connectors.get(account_name)
            if not connector:
                results.append(f"\u274c {account_name}: disconnected")
                continue

            display = next(
                (a.display_name for a in self.config.accounts if a.name == account_name),
                account_name,
            )

            # SAFETY RE-VALIDATION with live data
            nlv_all = await connector.get_nlv_by_currency()
            nlv_usd = nlv_all.get("USD", 0.0)
            order_cost = qty * option_price * 100
            order_pct = (order_cost / nlv_usd * 100) if nlv_usd > 0 else 100

            if order_pct > self.MAX_SINGLE_ORDER_PCT:
                results.append(
                    f"\u26a0\ufe0f {display}: BLOCKED \u2014 {order_pct:.1f}% exceeds "
                    f"{self.MAX_SINGLE_ORDER_PCT}% limit"
                )
                continue

            # Check total ticker exposure with live portfolio
            portfolio = await connector.get_portfolio()
            existing_value = sum(
                abs(p.marketValue) for p in portfolio if p.contract.symbol == ticker
            )
            total_pct = ((existing_value + order_cost) / nlv_usd * 100) if nlv_usd > 0 else 100
            if total_pct > self.MAX_TOTAL_POSITION_PCT:
                results.append(
                    f"\u26a0\ufe0f {display}: BLOCKED \u2014 would be {total_pct:.1f}% "
                    f"in {ticker} (limit {self.MAX_TOTAL_POSITION_PCT}%)"
                )
                continue

            # Check available funds (+ margin budget if configured)
            available = await connector.get_available_funds("USD")
            acfg = next(
                (a for a in self.config.accounts if a.name == account_name), None,
            )
            margin_on = (
                acfg and acfg.margin_mode in ("soft", "hard")
                and acfg.is_margin_account and acfg.max_margin_usd > 0
            )
            margin_cap = acfg.max_margin_usd if margin_on else 0
            budget = available + margin_cap
            if order_cost > budget:
                margin_note = f" + ${margin_cap:,.0f} margin" if margin_cap else ""
                results.append(
                    f"\u26a0\ufe0f {display}: BLOCKED \u2014 cost ${order_cost:,.0f} > "
                    f"available ${available:,.0f}{margin_note}"
                )
                continue

            # All checks passed — qualify and place
            contract = Option(
                ticker, order_details["expiry"],
                order_details["strike"], order_details["right"], "SMART"
            )
            qualified = await connector.qualify_contracts(contract)
            if not qualified:
                results.append(f"\u274c {display}: contract not found")
                continue

            if limit_price == "MKT":
                order = MarketOrder("BUY", qty)
                price_str = "MKT"
            else:
                order = LimitOrder("BUY", qty, limit_price)
                price_str = f"${limit_price:.2f}"
            order_id = await connector.place_order(qualified[0], order)

            results.append(
                f"\u2705 {display}: BUY {qty}x @ {price_str} (#{order_id})"
            )
            logger.info(f"[{account_name}] /buy placed: {qty}x {contract.localSymbol} @ {price_str}")

        await self.db.log_audit("manual_buy", ticker=ticker, detail=str(order_details))
        return "\n".join(results)

    async def _on_sell_preview(self, raw_args: str) -> tuple[str, dict]:
        """Parse sell args, look up position, preview proportional sell."""
        if not self.executor.connectors:
            raise ValueError("No IBKR accounts connected")

        parsed = self._parse_order_args(raw_args)
        ticker = parsed["ticker"]
        pct_spec = parsed["pct"]  # 'all', 'half', or a number (% of position)

        # Always sell from all accounts
        accounts = self._resolve_accounts("all")
        if not accounts:
            raise ValueError("No accounts connected")

        # Find existing positions for this ticker
        positions_by_account = {}
        for acfg in accounts:
            connector = self.executor.connectors.get(acfg.name)
            if not connector:
                continue
            positions = await connector.get_positions()
            matching = [p for p in positions if p.contract.symbol == ticker and p.position > 0]
            if matching:
                if len(matching) > 1:
                    logger.warning("Multiple positions for %s on %s, using first", ticker, acfg.name)
                positions_by_account[acfg.name] = matching[0]

        if not positions_by_account:
            raise ValueError(f"No open position found for {ticker}")

        # Calculate sell quantities (same % of each account's position)
        allocations = {}
        for acfg_name, pos in positions_by_account.items():
            held = int(pos.position)
            if isinstance(pct_spec, str):
                if pct_spec == "all":
                    sell_qty = held
                elif pct_spec == "half":
                    sell_qty = max(1, held // 2)
                elif pct_spec == "third":
                    sell_qty = max(1, held // 3)
                elif pct_spec == "quarter":
                    sell_qty = max(1, held // 4)
                else:
                    sell_qty = held
            else:
                # pct_spec is % of position to sell
                sell_qty = max(1, int(held * pct_spec / 100))
                sell_qty = min(sell_qty, held)  # Never sell more than held

            allocations[acfg_name] = sell_qty

        # Get price and market data from first position
        first_acct = next(iter(positions_by_account))
        first_pos = positions_by_account[first_acct]
        first_connector = self.executor.connectors[first_acct]
        detail = await first_connector.get_option_detail(first_pos.contract)
        option_price = detail["mid"] if detail["mid"] > 0 else detail["last"] or detail["close"]

        # Fall back to cached portfolio price when market is closed (all zeros)
        if not option_price or option_price <= 0:
            portfolio = await first_connector.get_portfolio()
            cached = next(
                (p for p in portfolio if p.contract.conId == first_pos.contract.conId),
                None,
            )
            if cached and cached.marketPrice and not math.isnan(cached.marketPrice):
                option_price = cached.marketPrice
                detail["bid"] = 0.0
                detail["ask"] = 0.0
                detail["spread_pct"] = 0.0

        if not option_price or option_price <= 0:
            raise ValueError(
                f"Cannot price {ticker} — market data unavailable. "
                f"Try again during market hours or check /info {ticker}"
            )
        value_per = option_price * 100

        total_sell_qty = sum(allocations.values())
        total_proceeds_usd = total_sell_qty * value_per
        usd_rate = await first_connector.get_exchange_rate("USD")
        total_proceeds_eur = total_proceeds_usd * usd_rate

        contract = first_pos.contract
        exp_str = contract.lastTradeDateOrContractMonth
        exp_display = f"{exp_str[4:6]}/{exp_str[0:4]}"

        limit_price = parsed["limit_price"]
        if limit_price == "MKT":
            price_label = "MARKET ORDER"
        else:
            price_label = f"Limit ${limit_price:.2f}"

        spread_warn = ""
        if detail["spread_pct"] > 5:
            spread_warn = "\n\u26a0\ufe0f <b>Wide spread!</b> Consider checking /info first."

        lines = [
            "\U0001f4e4 <b>SELL Order Preview</b>\n",
            f"<b>{ticker}</b> {contract.strike}{contract.right} exp {exp_display}",
            f"<code>{contract.localSymbol}</code>\n",
            f"<b>Market:</b> Bid ${detail['bid']:.2f} | Ask ${detail['ask']:.2f} | Spread {detail['spread_pct']:.1f}%",
            f"<b>Order:</b> {price_label}",
            f"<b>Total Sell:</b> {total_sell_qty} contracts",
            f"<b>Est. Proceeds:</b> ${total_proceeds_usd:,.0f} (\u20ac{total_proceeds_eur:,.0f}){spread_warn}\n",
            "<b>Per Account:</b>",
        ]
        for acfg in accounts:
            if acfg.name not in allocations:
                continue
            display = acfg.display_name or acfg.name
            qty_a = allocations[acfg.name]
            held = int(positions_by_account[acfg.name].position)
            lines.append(f"  {display}: sell {qty_a}/{held} contracts")

        text = "\n".join(lines)

        order_details = {
            "action": "SELL",
            "ticker": ticker,
            "expiry": exp_str,
            "strike": contract.strike,
            "right": contract.right,
            "option_price": option_price,
            "limit_price": limit_price,
            "allocations": allocations,
        }
        return text, order_details

    async def _on_sell_execute(self, order_details: dict) -> str:
        """Execute a confirmed sell order across accounts."""
        from ib_async import LimitOrder, MarketOrder

        ticker = order_details["ticker"]
        allocations = order_details["allocations"]
        limit_price = order_details["limit_price"]

        results = []
        for account_name, qty in allocations.items():
            if qty <= 0:
                continue
            connector = self.executor.connectors.get(account_name)
            if not connector:
                results.append(f"\u274c {account_name}: disconnected")
                continue

            positions = await connector.get_positions()
            matching = [p for p in positions if p.contract.symbol == ticker and p.position > 0]
            if not matching:
                results.append(f"\u274c {account_name}: position gone")
                continue

            held_qty = int(abs(matching[0].position))
            if qty > held_qty:
                logger.warning(
                    "Sell qty %d exceeds held %d for %s on %s, clamping",
                    qty, held_qty, ticker, account_name,
                )
                qty = held_qty
            if qty <= 0:
                logger.warning("No position to sell for %s on %s, skipping", ticker, account_name)
                continue

            contract = matching[0].contract
            if limit_price == "MKT":
                order = MarketOrder("SELL", qty)
                price_str = "MKT"
            else:
                order = LimitOrder("SELL", qty, limit_price)
                price_str = f"${limit_price:.2f}"
            order_id = await connector.place_order(contract, order)

            display = next(
                (a.display_name for a in self.config.accounts if a.name == account_name),
                account_name,
            )
            results.append(
                f"\u2705 {display}: SELL {qty}x @ {price_str} (#{order_id})"
            )
            logger.info(f"[{account_name}] /sell placed: {qty}x {contract.localSymbol} @ {price_str}")

        await self.db.log_audit("manual_sell", ticker=ticker, detail=str(order_details))
        return "\n".join(results)

    async def _on_info_requested(self, ticker: str) -> str:
        """Show full position details: qty, cost, bid/ask/spread, Greeks, day P&L."""
        if not self.executor.connectors:
            return "\u26a0\ufe0f No IBKR accounts connected"

        ticker = ticker.strip().upper()
        lines = [f"\U0001f4cb <b>Position Info: {ticker}</b>\n"]

        for cfg in self.config.accounts:
            connector = self.executor.connectors.get(cfg.name)
            if not connector:
                continue
            display = cfg.display_name or cfg.name
            positions = await connector.get_positions()
            matching = [p for p in positions if p.contract.symbol == ticker and p.position > 0]
            if not matching:
                lines.append(f"<b>{display}</b>: no position")
                continue

            # Get cached portfolio data as fallback (has last known prices)
            portfolio = await connector.get_portfolio()
            portfolio_by_conid = {
                item.contract.conId: item for item in portfolio
            }

            for pos in matching:
                contract = pos.contract
                qty = int(pos.position)
                avg_cost = pos.avgCost / 100  # IBKR stores total cost per share, options x100
                exp = contract.lastTradeDateOrContractMonth or ""
                exp_display = f"{exp[4:6]}/{exp[0:4]}" if len(exp) >= 6 else exp

                # Try live snapshot first
                try:
                    detail = await connector.get_option_detail(contract)
                except Exception:
                    detail = {"bid": 0, "ask": 0, "mid": 0, "spread": 0,
                              "spread_pct": 0, "delta": None, "gamma": None,
                              "theta": None, "iv": None}

                # Fall back to cached portfolio data if snapshot returned zeros
                cached = portfolio_by_conid.get(contract.conId)
                using_cached = False
                if detail["bid"] == 0 and detail["ask"] == 0 and cached:
                    cached_price = cached.marketPrice
                    if cached_price and not math.isnan(cached_price):
                        detail["mid"] = cached_price
                        using_cached = True

                header = f"{contract.strike}{contract.right} exp {exp_display}"
                lines.append(f"\n<b>{display}</b> \u2014 {header}")
                lines.append(f"  Qty: <b>{qty}</b> | Avg: ${avg_cost:.2f}")

                if using_cached:
                    lines.append(
                        f"  Price: ${detail['mid']:.2f} (cached — market closed)"
                    )
                else:
                    lines.append(
                        f"  Bid: ${detail['bid']:.2f} | Ask: ${detail['ask']:.2f} | "
                        f"Mid: ${detail['mid']:.2f}"
                    )
                    spread_pct = detail['spread_pct']
                    spread_warn = " \u26a0\ufe0f WIDE" if spread_pct > 5 else ""
                    lines.append(
                        f"  Spread: ${detail['spread']:.2f} ({spread_pct:.1f}%){spread_warn}"
                    )

                if detail['delta'] is not None:
                    if detail['iv']:
                        lines.append(
                            f"  \u0394={detail['delta']:.3f} | "
                            f"\u0398={detail['theta']:.3f} | "
                            f"IV={detail['iv']*100:.1f}%"
                        )
                    else:
                        lines.append(
                            f"  \u0394={detail['delta']:.3f} | \u0398={detail['theta']:.3f}"
                        )

                # Use cached P&L if available, otherwise calculate from mid
                if cached and using_cached:
                    mkt_value = cached.marketValue
                    pnl = cached.unrealizedPNL
                else:
                    mkt_value = qty * detail['mid'] * 100
                    pnl = mkt_value - (qty * avg_cost * 100)
                cost_basis = qty * avg_cost * 100
                pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0
                arrow = "\u25b2" if pnl >= 0 else "\u25bc"
                lines.append(
                    f"  Value: ${mkt_value:,.0f} | P&L: {arrow} ${abs(pnl):,.0f} ({pnl_pct:+.1f}%)"
                )

        return "\n".join(lines)

    async def _on_list_positions(self) -> list[dict]:
        """Return unique tickers currently held (for quick /buy and /sell buttons)."""
        if not self.executor.connectors:
            return []

        seen = {}  # ticker -> {symbol, total_qty, contract_desc}
        for cfg in self.config.accounts:
            connector = self.executor.connectors.get(cfg.name)
            if not connector:
                continue
            positions = await connector.get_positions()
            for p in positions:
                if p.position <= 0:
                    continue
                sym = p.contract.symbol
                if sym not in seen:
                    exp = p.contract.lastTradeDateOrContractMonth or ""
                    strike = p.contract.strike or 0
                    right = p.contract.right or ""
                    exp_display = f"{exp[4:6]}/{exp[2:4]}" if len(exp) >= 6 else exp
                    desc = f"{strike}{right} {exp_display}" if strike else sym
                    seen[sym] = {"symbol": sym, "total_qty": 0, "desc": desc}
                seen[sym]["total_qty"] += int(p.position)
        return list(seen.values())

    async def _on_option_expiries(self, ticker: str) -> list[dict]:
        """Get available option expiries for a ticker (LEAPS only, >1yr out)."""

        connector = next(iter(self.executor.connectors.values()), None)
        if not connector:
            raise ValueError("No IBKR connection")

        from ib_async import Stock
        stock = Stock(ticker, "SMART", "USD")
        qualified = await connector.qualify_contracts(stock)
        if not qualified:
            raise ValueError(f"Unknown ticker: {ticker}")

        chains = await connector.get_option_chain_params(
            qualified[0].symbol, qualified[0].secType, qualified[0].conId,
        )
        if not chains:
            raise ValueError(f"No option chains for {ticker}")

        chain = next((c for c in chains if c.exchange == "SMART"), chains[0])

        now = datetime.now(UTC)
        min_dte = 180  # Show anything >6 months for flexibility
        expiries = []
        for exp_str in sorted(chain.expirations):
            exp_date = datetime.strptime(exp_str, "%Y%m%d").replace(tzinfo=UTC)
            dte = (exp_date - now).days
            if dte >= min_dte:
                display = exp_date.strftime("%b'%y")
                expiries.append({"exp": exp_str, "display": display, "dte": dte})

        if not expiries:
            raise ValueError(f"No long-dated expiries for {ticker}")
        return expiries

    async def _on_option_strikes(self, ticker: str, expiry: str, right: str = "C") -> dict:
        """Get strikes for a ticker/expiry with context (current price, ITM/OTM labels)."""
        connector = next(iter(self.executor.connectors.values()), None)
        if not connector:
            raise ValueError("No IBKR connection")

        from ib_async import Stock
        stock = Stock(ticker, "SMART", "USD")
        qualified = await connector.qualify_contracts(stock)
        if not qualified:
            raise ValueError(f"Unknown ticker: {ticker}")

        chains = await connector.get_option_chain_params(
            qualified[0].symbol, qualified[0].secType, qualified[0].conId,
        )
        chain = next((c for c in chains if c.exchange == "SMART"), chains[0])

        # Only keep strikes that have this expiry
        # Get current price for ITM/OTM labeling
        current_price = await connector.get_current_price(ticker)

        all_strikes = sorted(chain.strikes)

        # For calls: ITM = strike < price. Show a useful subset.
        # Focus on: deep ITM (delta~0.9+), ITM, ATM, slightly OTM
        if right == "C":
            itm = [s for s in all_strikes if s <= current_price]
            otm = [s for s in all_strikes if s > current_price]
        else:
            itm = [s for s in all_strikes if s >= current_price]
            otm = [s for s in all_strikes if s < current_price]

        # Show: 6 deepest ITM + 4 near ATM + 3 OTM (max ~13 buttons)
        deep_itm = itm[:6] if right == "C" else itm[-6:]
        near_atm = itm[-4:] if right == "C" else itm[:4]
        near_otm = otm[:3] if right == "C" else otm[-3:]

        # Deduplicate and keep sorted
        selected = sorted(set(deep_itm + near_atm + near_otm))

        strikes = []
        for s in selected:
            if right == "C":
                label = "ITM" if s < current_price else ("ATM" if s == current_price else "OTM")
            else:
                label = "ITM" if s > current_price else ("ATM" if s == current_price else "OTM")
            strikes.append({"strike": s, "label": label})

        return {
            "ticker": ticker,
            "expiry": expiry,
            "right": right,
            "current_price": current_price,
            "strikes": strikes,
        }

    def _resolve_accounts(self, account_filter: str) -> list:
        """Resolve account filter to list of AccountConfig objects."""
        result = []
        for cfg in self.config.accounts:
            if cfg.name not in self.executor.connectors:
                continue
            if account_filter == "all":
                result.append(cfg)
            elif account_filter == cfg.name:
                result.append(cfg)
            elif cfg.display_name and account_filter == cfg.display_name.lower():
                result.append(cfg)
        return result

    async def _on_account_requested(self, account_filter: str) -> str:
        """Called when admin requests /account — show individual account detail."""
        if not self.executor.connectors:
            return "No IBKR accounts connected"

        accounts_to_show = self._resolve_accounts(account_filter)

        if not accounts_to_show:
            return f"Account '{account_filter}' not found. Use account names from config.yaml"

        lines = []
        for account_cfg in accounts_to_show:
            name = account_cfg.name
            display = account_cfg.display_name or name
            connector = self.executor.connectors.get(name)
            if not connector:
                lines.append(f"\u26a0\ufe0f <b>{display}</b>: disconnected\n")
                continue

            nlv_all = await connector.get_nlv_by_currency()
            nlv_eur = nlv_all.get("EUR", 0.0)
            deposits = account_cfg.net_deposits
            total_return = nlv_eur - deposits
            return_pct = (total_return / deposits * 100) if deposits > 0 else 0

            portfolio = await connector.get_portfolio()
            lines.append(
                f"\U0001f464 <b>{display}</b>\n"
                f"NLV: \u20ac{nlv_eur:,.0f} | Net Deposited: \u20ac{deposits:,.0f}\n"
                f"Return: \u20ac{total_return:+,.0f} ({return_pct:+.1f}%)\n"
                f"Positions: {len(portfolio)}\n"
            )
            if portfolio:
                lines.append("<pre>")
                for item in sorted(portfolio, key=lambda x: abs(x.unrealizedPNL), reverse=True):
                    sym = item.contract.symbol
                    pnl = item.unrealizedPNL
                    qty = int(item.position)
                    emoji = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
                    lines.append(f"{emoji} {sym:6} {qty:3}x  PnL ${pnl:+,.0f}")
                lines.append("</pre>")
            lines.append("")

        return "\n".join(lines)

    async def _on_signals_requested(self, limit: int = 10) -> str:
        """Called when admin requests /signals — show recent signals."""
        signals = await self.db.get_recent_signals(limit)
        if not signals:
            return "No signals recorded yet."

        lines = [f"\U0001f4e1 <b>Last {len(signals)} Signals</b>\n"]
        for sig in signals:
            status_emoji = {
                "executed": "\u2705", "confirmed": "\u23f3", "pending": "\U0001f7e1",
                "skipped": "\u23ed\ufe0f", "failed": "\u274c",
            }.get(sig.get("status", ""), "\u2753")
            ticker = sig.get("ticker", "?")
            action = sig.get("action", "?")
            status = sig.get("status", "?")
            created = sig.get("created_at", "")[:16]
            lines.append(f"{status_emoji} <b>${ticker}</b> {action} [{status}] {created}")

        return "\n".join(lines)

    async def _on_deposits_requested(self, account_filter: str = "all") -> str:
        """Called when admin requests /deposits — show deposit/withdrawal history."""
        lines = ["\U0001f4b0 <b>Deposit / Withdrawal History</b>\n"]

        grand_in = 0.0
        grand_out = 0.0
        grand_baseline = 0.0

        for account_cfg in self.config.accounts:
            display = account_cfg.display_name or account_cfg.name
            if account_filter not in ("all", account_cfg.name, display.lower()):
                continue

            baseline = self._deposit_baselines.get(account_cfg.name, 0)
            grand_baseline += baseline
            txns = await self.db.get_cash_transactions(account_cfg.name)

            lines.append(f"\U0001f464 <b>{display}</b>")
            if baseline:
                lines.append(f"  <i>Pre-Flex baseline (net): \u20ac{baseline:,.0f}</i>")

            if not txns and not baseline:
                lines.append("  No transactions recorded yet.\n")
                continue

            total_in = 0.0
            total_out = 0.0
            for txn in txns:
                amount = txn["amount"]
                date = txn["report_date"]
                if len(date) == 8:
                    date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
                if amount >= 0:
                    emoji = "\U0001f7e2"
                    total_in += amount
                else:
                    emoji = "\U0001f534"
                    total_out += amount
                lines.append(f"  {emoji} {date}  \u20ac{amount:+,.2f}")

            grand_in += total_in
            grand_out += total_out

            if txns:
                lines.append(
                    f"\n  Tracked deposits: \u20ac{total_in:,.0f}"
                )
                if total_out:
                    lines.append(f"  Tracked withdrawals: \u20ac{total_out:,.0f}")
            net = total_in + total_out
            total_net = baseline + net
            lines.append(f"  <b>Total net deposited: \u20ac{total_net:,.0f}</b>")
            lines.append("")

        # Combined summary if showing multiple accounts
        if account_filter == "all" and len(self.config.accounts) > 1:
            combined_net = grand_baseline + grand_in + grand_out
            lines.append(
                f"\U0001f3af <b>Combined</b>\n"
                f"  Pre-Flex baselines: \u20ac{grand_baseline:,.0f}\n"
                f"  Tracked deposits: \u20ac{grand_in:,.0f}\n"
                f"  Tracked withdrawals: \u20ac{grand_out:,.0f}\n"
                f"  <b>Total net deposited: \u20ac{combined_net:,.0f}</b>"
            )

        return "\n".join(lines)

    async def _on_health_requested(self) -> str:
        """Called when admin requests /status — full system health."""
        lines = ["\U0001f3e5 <b>System Health</b>\n"]

        # Gateway connectivity
        connected = []
        disconnected = []
        for cfg in self.config.accounts:
            connector = self.executor.connectors.get(cfg.name)
            display = html_escape(cfg.display_name or cfg.name)
            if connector and connector.is_connected:
                connected.append(display)
            else:
                disconnected.append(display)

        if connected:
            lines.append(f"\U0001f7e2 Gateways: {', '.join(connected)}")
        if disconnected:
            lines.append(f"\U0001f534 Disconnected: {', '.join(disconnected)}")

        # Bot
        lines.append("\U0001f4e8 Telegram bot: active")

        # Pending
        lines.append(f"\u23f3 Pending confirmations: {self.bot.get_pending_count()}")

        # Last sync (from DB)
        try:
            last_sync = await self.db.get_last_sync_time()
            if last_sync:
                lines.append(f"\U0001f504 Last sync: {last_sync[:19]}")
        except Exception:
            pass

        # Webhook status
        if self.webhook:
            if self.webhook.last_signal_at:
                ts = self.webhook.last_signal_at.strftime("%Y-%m-%d %H:%M:%S")
                lines.append(f"\U0001f310 Webhook: active ({self.webhook.total_processed} signals, last: {ts})")
            else:
                lines.append("\U0001f310 Webhook: listening (no signals yet)")
        else:
            lines.append("\U0001f310 Webhook: disabled")

        # Optional web dashboard link
        if self.config.web_url:
            lines.append(f"\n\U0001f517 <a href=\"{html_escape(self.config.web_url)}\">Web Dashboard</a>")

        return "\n".join(lines)

    async def _sync_flex_deposits(self) -> None:
        """Pull deposit/withdrawal history from IBKR Flex Web Service.

        IBKR limits Flex Queries to max 365 calendar days.  Each individual
        transaction is persisted in the DB so it survives beyond the rolling
        window.  ``config.net_deposits`` acts as a historical baseline for
        any deposits/withdrawals that predate the *first* Flex sync.

        Total = config.net_deposits (baseline) + SUM(cash_transactions in DB)
        """
        # Fetch flex transactions concurrently across accounts
        flex_accounts = [
            acc for acc in self.config.accounts
            if acc.flex_token and acc.flex_query_id
        ]
        if not flex_accounts:
            return

        fetch_tasks = [
            asyncio.to_thread(self._fetch_flex_transactions, acc.flex_token, acc.flex_query_id)
            for acc in flex_accounts
        ]
        fetch_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for account_cfg, result in zip(flex_accounts, fetch_results):
            try:
                if isinstance(result, Exception):
                    raise result
                transactions = result
                if transactions is not None:
                    # Store each transaction in the DB (dedup via UNIQUE constraint)
                    for txn in transactions:
                        await self.db.upsert_cash_transaction(
                            account_name=account_cfg.name,
                            report_date=txn["date"],
                            amount=txn["amount"],
                            currency=txn["currency"],
                            description=txn["description"],
                        )

                # Total = original config baseline + all persisted transactions
                db_net = await self.db.get_net_deposits(account_cfg.name)
                baseline = self._deposit_baselines.get(account_cfg.name, 0)
                account_cfg.net_deposits = baseline + db_net
                logger.info(
                    f"Flex sync [{account_cfg.display_name or account_cfg.name}]: "
                    f"baseline=\u20ac{baseline:,.2f} + db=\u20ac{db_net:,.2f} "
                    f"= total \u20ac{account_cfg.net_deposits:,.2f}"
                )
                # Update account_summary so dashboard reflects correct deposits
                await self.db.update_account_deposits(account_cfg.name, account_cfg.net_deposits)
            except Exception as e:
                logger.error(f"Flex sync failed for {account_cfg.name}: {e}")

    @staticmethod
    def _fetch_flex_transactions(token: str, query_id: int) -> list[dict] | None:
        """Fetch deposit/withdrawal transactions from IBKR Flex (blocking)."""
        import time

        tls_ctx = ssl.create_default_context()

        # Step 1: Request the report
        url1 = f"{FLEX_REQUEST_URL}?t={token}&q={query_id}&v=3"
        resp1 = urllib.request.urlopen(url1, timeout=30, context=tls_ctx).read().decode()
        root1 = DefusedET.fromstring(resp1)
        status = root1.findtext("Status", "")
        if status != "Success":
            err = root1.findtext("ErrorMessage", "unknown")
            logger.warning(f"Flex request failed: {err}")
            return None

        ref_code = root1.findtext("ReferenceCode", "")
        if not ref_code:
            return None

        # Step 2: Retrieve (with retries)
        for attempt in range(5):
            time.sleep(5 * (attempt + 1))
            url2 = f"{FLEX_STATEMENT_URL}?q={ref_code}&t={token}&v=3"
            resp2 = urllib.request.urlopen(url2, timeout=30, context=tls_ctx).read().decode()

            if "Statement generation in progress" in resp2:
                continue

            root2 = DefusedET.fromstring(resp2)

            # Extract individual deposit/withdrawal transactions
            transactions = []
            seen = set()
            for ct in root2.iter("CashTransaction"):
                if ct.get("type") != "Deposits/Withdrawals":
                    continue
                report_date = ct.get("reportDate", "")
                amount_str = ct.get("amount", "0")
                description = ct.get("description", "")
                currency = ct.get("currency", "EUR")
                # Deduplicate (CashTransactions + StmtFunds overlap)
                key = (report_date, amount_str, description)
                if key in seen:
                    continue
                seen.add(key)
                transactions.append({
                    "date": report_date,
                    "amount": float(amount_str),
                    "currency": currency,
                    "description": description,
                })

            if transactions:
                dates = [t["date"] for t in transactions]
                net = sum(t["amount"] for t in transactions)
                logger.info(
                    f"Flex data covers {min(dates)} to {max(dates)} "
                    f"({len(transactions)} transactions, net={net:,.2f})"
                )
            else:
                logger.info("Flex query returned no deposit/withdrawal transactions")

            return transactions

        logger.warning(f"Flex report not ready after retries (ref={ref_code})")
        return None

    async def _sync_positions(self) -> None:
        """Sync current IBKR positions and account data into the database."""
        for name, connector in self.executor.connectors.items():
            try:
                # Use portfolio() for market data (price, value, PnL)
                portfolio = await connector.get_portfolio()

                # If connected but portfolio/accountValues empty, re-subscribe
                # (ib_async's reqAccountUpdates subscription can drop silently)
                if not portfolio and connector.is_connected:
                    accounts = connector.managed_accounts()
                    if accounts:
                        logger.warning(
                            f"[{name}] Connected but portfolio empty — "
                            f"re-subscribing to account updates for {accounts[0]}"
                        )
                        connector.req_account_updates(True, accounts[0])
                        await asyncio.sleep(2)
                        portfolio = await connector.get_portfolio()
                # Get NLV in all available currencies + exchange rate
                nlv_all = await connector.get_nlv_by_currency()
                nlv_usd = nlv_all.get("USD", 0.0)

                total_market_value = 0.0
                total_pnl = 0.0
                for item in portfolio:
                    ticker = item.contract.localSymbol or item.contract.symbol
                    total_market_value += item.marketValue
                    total_pnl += item.unrealizedPNL
                    weight_pct = (abs(item.marketValue) / nlv_usd * 100) if nlv_usd > 0 else 0.0
                    await self.db.upsert_position(
                        account_name=name,
                        ticker=ticker,
                        quantity=int(item.position),
                        avg_cost=item.averageCost,
                        current_price=item.marketPrice,
                        weight_pct=weight_pct,
                        pnl=item.unrealizedPNL,
                    )

                # Remove positions no longer held from DB
                active_tickers = {
                    item.contract.localSymbol or item.contract.symbol
                    for item in portfolio
                }
                deleted = await self.db.delete_stale_positions(name, active_tickers)
                if deleted:
                    logger.info(f"[{name}] Cleaned {deleted} stale position(s) from DB")
                nlv = next(iter(nlv_all.values()), 0.0)
                base_currency = next(iter(nlv_all.keys()), "USD")
                nlv_eur = nlv_all.get("EUR", 0.0)
                nlv_usd = nlv_all.get("USD", 0.0)

                # Get EUR/USD exchange rate from IBKR accountValues
                # For EUR-base accounts: rate = EUR per 1 USD (e.g. 0.88)
                eur_per_usd = await connector.get_exchange_rate("USD")
                if nlv_eur > 0 and nlv_usd == 0 and eur_per_usd > 0:
                    nlv_usd = nlv_eur / eur_per_usd
                elif nlv_usd > 0 and nlv_eur == 0 and eur_per_usd > 0:
                    nlv_eur = nlv_usd * eur_per_usd

                # Find config for this account to get net_deposits/display_name
                account_cfg = next(
                    (a for a in self.config.accounts if a.name == name), None
                )
                await self.db.upsert_account_summary(
                    name, nlv, total_market_value, total_pnl,
                    base_currency=base_currency,
                    nlv_eur=nlv_eur,
                    nlv_usd=nlv_usd,
                    exchange_rate=eur_per_usd,
                    net_deposits=account_cfg.net_deposits if account_cfg else 0,
                    display_name=account_cfg.display_name if account_cfg else "",
                )
                await self.db.snapshot_nlv(
                    name, nlv_eur, nlv_usd,
                    net_deposits=account_cfg.net_deposits if account_cfg else 0,
                )
                logger.info(
                    f"Synced {len(portfolio)} positions from {name} "
                    f"(NLV={base_currency} {nlv:,.0f}, EUR={nlv_eur:,.0f}, "
                    f"USD={nlv_usd:,.0f}, FX={eur_per_usd:.4f}, "
                    f"MV={total_market_value:,.0f}, PnL={total_pnl:,.0f})"
                )
            except Exception as e:
                logger.error(f"Failed to sync positions from {name}: {e}")

    async def _periodic_sync(self) -> None:
        """Re-sync positions every 60s and Flex deposits every hour."""
        cycle = 0
        while True:
            await asyncio.sleep(60)
            if self._gateway_paused:
                continue
            try:
                await self._sync_positions()
                # Refresh PnL subscriptions (picks up new positions)
                for connector in self.executor.connectors.values():
                    await connector.subscribe_pnl()
                # Check margin compliance on every sync cycle
                await self._check_margin_compliance()
            except Exception as e:
                logger.error(f"Periodic sync failed: {e}")
            cycle += 1
            if cycle % 60 == 0:  # Every hour
                try:
                    await self._sync_flex_deposits()
                except Exception as e:
                    logger.error(f"Periodic Flex sync failed: {e}")

    async def _on_pause_requested(self, minutes: int) -> str:
        """Stop IB Gateway containers so the user can log into IBKR."""
        if self._gateway_paused:
            return "\u23f8 Gateways are already paused."
        self._gateway_paused = True
        loop = asyncio.get_running_loop()
        try:
            client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
            stopped = []
            for name in self._gateway_containers:
                try:
                    container = client.containers.get(name)
                    await loop.run_in_executor(None, container.stop)
                    stopped.append(name)
                except Exception as e:
                    logger.error(f"Failed to stop {name}: {e}")
            client.close()
        except Exception as e:
            self._gateway_paused = False
            logger.error(f"Docker connect failed: {e}")
            return f"\u274c Failed to connect to Docker: {e}"

        # Schedule auto-resume
        if self._resume_task and not self._resume_task.done():
            self._resume_task.cancel()
        self._resume_task = asyncio.ensure_future(self._auto_resume(minutes))

        logger.info(f"Gateways paused for {minutes} minutes: {stopped}")
        return (
            f"\u23f8 <b>Gateways paused for {minutes} minutes</b>\n\n"
            f"Stopped: {', '.join(stopped)}\n"
            "You can now log into IBKR freely.\n"
            "Gateways will auto-resume, or press Resume Now."
        )

    async def _auto_resume(self, minutes: int) -> None:
        """Wait N minutes then restart gateways."""
        try:
            await asyncio.sleep(minutes * 60)
            result = await self._on_resume_requested()
            await self.bot.send_notification(result)
        except asyncio.CancelledError:
            pass  # Manual resume cancelled the timer

    async def _on_resume_requested(self) -> str:
        """Restart IB Gateway containers and reconnect."""
        if not self._gateway_paused:
            return "\u25b6\ufe0f Gateways are already running."
        loop = asyncio.get_running_loop()
        try:
            client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
            started = []
            for name in self._gateway_containers:
                try:
                    container = client.containers.get(name)
                    await loop.run_in_executor(None, container.start)
                    started.append(name)
                except Exception as e:
                    logger.error(f"Failed to start {name}: {e}")
            client.close()
        except Exception as e:
            logger.error(f"Docker connect failed: {e}")
            return f"\u274c Failed to connect to Docker: {e}"

        self._gateway_paused = False
        if self._resume_task and not self._resume_task.done():
            self._resume_task.cancel()
            self._resume_task = None

        # Wait for gateways to initialize, then reconnect
        await asyncio.sleep(15)
        try:
            await self.executor.connect_all()
            for connector in self.executor.connectors.values():
                await connector.subscribe_pnl()
            await self._sync_positions()
        except Exception as e:
            logger.error(f"Reconnect after resume failed: {e}")
            return f"\u26a0\ufe0f Gateways started but reconnect failed: {e}"

        logger.info(f"Gateways resumed: {started}")
        return (
            f"\u25b6\ufe0f <b>Gateways resumed</b>\n\n"
            f"Started: {', '.join(started)}\n"
            "Reconnected and P&amp;L subscriptions active."
        )

    async def _on_order_event(self, account_name: str, info: dict) -> None:
        """Called on any order status change — notify via Telegram."""
        display = html_escape(next(
            (a.display_name for a in self.config.accounts if a.name == account_name),
            account_name,
        ))
        event = info.get("event", "unknown")
        symbol = info.get("symbol", "?")
        local_sym = html_escape(info.get("local_symbol", symbol))
        action = info.get("action", "?")
        qty = info.get("qty", 0)
        avg_price = info.get("avg_price", 0.0)
        order_id = info.get("order_id", 0)

        if event == "submitted":
            text = (
                f"\U0001f4e8 <b>ORDER ACCEPTED</b>\n\n"
                f"<b>{display}</b>: {action} {qty}x <code>{local_sym}</code>\n"
                f"Order #{order_id} working at exchange"
            )
        elif event == "filled":
            proceeds = qty * avg_price * 100
            usd_rate = 1.0
            connector = self.executor.connectors.get(account_name)
            if connector:
                usd_rate = await connector.get_exchange_rate("USD")
            proceeds_eur = proceeds * usd_rate
            text = (
                f"\u2705 <b>ORDER FILLED</b>\n\n"
                f"<b>{display}</b>: {action} {qty}x <code>{local_sym}</code>\n"
                f"Avg Price: ${avg_price:.2f}\n"
                f"Value: ${proceeds:,.0f} (\u20ac{proceeds_eur:,.0f})\n"
                f"Order #{order_id}"
            )
            await self.db.log_audit("order_filled", ticker=symbol,
                                    detail=f"{display}: {action} {qty}x @ ${avg_price:.2f}")
            # Update execution record with actual fill data
            exec_row = await self.db.find_execution_by_order(account_name, order_id)
            if exec_row:
                nlv_usd = 0.0
                if connector:
                    nlv_all = await connector.get_nlv_by_currency()
                    nlv_usd = nlv_all.get("USD", 0.0)
                actual_pct = (proceeds / nlv_usd * 100) if nlv_usd > 0 else 0.0
                await self.db.update_execution_fill(
                    exec_row["id"], qty, avg_price, "filled",
                )
                await self.db.update_execution_allocation(exec_row["id"], actual_pct)
        elif event == "rejected":
            error = info.get("error", "Unknown reason")
            text = (
                f"\U0001f6ab <b>ORDER REJECTED</b>\n\n"
                f"<b>{display}</b>: {action} {qty}x <code>{local_sym}</code>\n"
                f"Reason: {error}\n"
                f"Order #{order_id}"
            )
            await self.db.log_audit("order_rejected", ticker=symbol,
                                    detail=f"{display}: {action} {qty}x — {error}")
        elif event == "cancelled":
            text = (
                f"\u274c <b>ORDER CANCELLED</b>\n\n"
                f"<b>{display}</b>: {action} {qty}x <code>{local_sym}</code>\n"
                f"Order #{order_id}"
            )
            await self.db.log_audit("order_cancelled", ticker=symbol,
                                    detail=f"{display}: {action} {qty}x cancelled")
        else:
            return

        logger.info(f"Order event [{event}]: {display} {action} {qty}x {local_sym}")
        if self.config.bot_token:
            await self.bot.send_notification(text)

        # After a buy fill, check margin compliance
        if event == "filled" and action == "BUY":
            await self._check_margin_compliance(account_name)

    async def _check_margin_compliance(self, account_name: str | None = None) -> None:
        """Check margin usage against caps.

        - soft mode: alert only (no auto-sell)
        - hard mode: auto-sell smallest position to restore compliance
        - off / no cap (max_margin_usd=0): skip

        If account_name is None, checks all accounts.
        """
        from ib_async import MarketOrder

        accounts = self.config.accounts
        if account_name:
            accounts = [a for a in accounts if a.name == account_name]

        for acfg in accounts:
            if acfg.margin_mode == "off" or acfg.max_margin_usd <= 0:
                continue
            connector = self.executor.connectors.get(acfg.name)
            if not connector or not connector.is_connected:
                continue

            margin_used = await connector.get_margin_used()
            if margin_used <= acfg.max_margin_usd:
                continue

            excess = margin_used - acfg.max_margin_usd
            display = acfg.display_name or acfg.name
            mode_label = acfg.margin_mode.upper()

            logger.warning(
                "[%s] MARGIN %s BREACH: $%.0f used > $%.0f cap (excess $%.0f)",
                acfg.name, mode_label, margin_used, acfg.max_margin_usd, excess,
            )
            await self.db.log_audit(
                "margin_breach", ticker="",
                detail=f"{display} [{mode_label}]: "
                       f"${margin_used:,.0f} > cap ${acfg.max_margin_usd:,.0f}",
            )

            if acfg.margin_mode == "soft":
                # Soft: alert only, no auto-sell
                alert = (
                    f"\u26a0\ufe0f <b>MARGIN WARNING — {display}</b>\n\n"
                    f"Margin used: ${margin_used:,.0f}\n"
                    f"Soft cap: ${acfg.max_margin_usd:,.0f}\n"
                    f"Excess: ${excess:,.0f}\n"
                    f"<i>No action taken (soft mode)</i>"
                )
                if self.config.bot_token:
                    await self.bot.send_notification(alert)
                continue

            # Hard mode: auto-sell to restore compliance
            alert = (
                f"\U0001f6a8 <b>MARGIN BREACH — {display}</b>\n\n"
                f"Margin used: ${margin_used:,.0f}\n"
                f"Hard cap: ${acfg.max_margin_usd:,.0f}\n"
                f"Excess: ${excess:,.0f}\n\n"
                f"Auto-selling smallest position to restore compliance..."
            )
            if self.config.bot_token:
                await self.bot.send_notification(alert)

            # Find smallest position to sell (minimize market impact)
            portfolio = await connector.get_portfolio()
            positions = [
                p for p in portfolio
                if p.position > 0 and p.marketValue > 0
            ]
            if not positions:
                logger.error("[%s] No positions to sell for margin recovery", acfg.name)
                continue

            positions.sort(key=lambda p: p.marketValue)

            # Sell enough contracts from smallest position to cover excess
            target = positions[0]
            contract = target.contract
            price_per = target.marketPrice
            value_per = price_per * 100  # options multiplier
            qty_to_sell = max(1, math.ceil(excess / value_per)) if value_per > 0 else 1
            qty_to_sell = min(qty_to_sell, int(target.position))

            logger.critical(
                "[%s] HARD auto-selling %d x %s to recover $%.0f margin",
                acfg.name, qty_to_sell, contract.localSymbol, excess,
            )

            order = MarketOrder("SELL", qty_to_sell)
            order_id = await connector.place_order(contract, order)

            # Wait for fill (up to 30s) instead of fixed sleep
            trades = []
            for _ in range(30):
                await asyncio.sleep(1)
                trades = [t for t in connector.get_trades()
                          if t.order.orderId == order_id]
                if trades and trades[0].orderStatus.status == "Filled":
                    break

            trade = trades[0] if trades else None
            fill_status = trade.orderStatus.status if trade else "Unknown"
            fill_price = trade.orderStatus.avgFillPrice if trade else 0.0

            result_text = (
                f"\U0001f6a8 <b>MARGIN AUTO-SELL — {display}</b>\n\n"
                f"Sold {qty_to_sell}x <code>{contract.localSymbol}</code>\n"
                f"Status: {fill_status}\n"
                f"Avg Price: ${fill_price:.2f}\n"
                f"Reason: margin ${margin_used:,.0f} exceeded hard cap "
                f"${acfg.max_margin_usd:,.0f}"
            )
            if self.config.bot_token:
                await self.bot.send_notification(result_text)
            await self.db.log_audit(
                "margin_auto_sell", ticker=contract.symbol,
                detail=f"{display}: sold {qty_to_sell}x {contract.localSymbol} "
                       f"(margin recovery ${excess:,.0f})",
            )

    async def _on_orders_requested(self) -> tuple[str, list[dict]]:
        """Return formatted open orders list + raw data for cancel buttons."""
        if not self.executor.connectors:
            return "\u26a0\ufe0f No IBKR accounts connected", []

        all_orders = []
        lines = ["\U0001f4cb <b>Open Orders</b>\n"]

        for cfg in self.config.accounts:
            connector = self.executor.connectors.get(cfg.name)
            if not connector:
                continue
            display = cfg.display_name or cfg.name
            orders = connector.get_open_orders()
            if not orders:
                lines.append(f"<b>{display}</b>: no open orders")
                continue

            lines.append(f"<b>{display}</b>:")
            for o in orders:
                price_str = f"${o['limit_price']:.2f}" if o['limit_price'] else "MKT"
                filled_str = f" ({o['filled']}/{o['qty']} filled)" if o['filled'] > 0 else ""
                lines.append(
                    f"  #{o['order_id']} {o['action']} {o['qty']}x "
                    f"<code>{o['local_symbol']}</code> @ {price_str} "
                    f"[{o['status']}]{filled_str}"
                )
                all_orders.append({**o, "account_name": cfg.name, "display": display})

        if not all_orders:
            lines = ["\U0001f4cb <b>Open Orders</b>\n\nNo open orders on any account."]

        return "\n".join(lines), all_orders

    async def _on_trades_requested(self, period: str = "today") -> str:
        """Return formatted trade execution history."""
        from datetime import timedelta

        now = datetime.now(UTC)
        if period == "week":
            since = (now - timedelta(days=7)).strftime("%Y-%m-%d")
            title = "Past 7 Days"
        else:
            since = now.strftime("%Y-%m-%d")
            title = "Today"

        execs = await self.db.get_executions_since(since)
        if not execs:
            return f"\U0001f4c8 <b>Trades \u2014 {title}</b>\n\nNo trades found."

        display_map = {
            cfg.name: cfg.display_name or cfg.name
            for cfg in self.config.accounts
        }

        lines = [f"\U0001f4c8 <b>Trades \u2014 {title}</b> ({len(execs)})\n"]

        # Group by date
        current_date = ""
        for ex in execs:
            ex_date = ex["created_at"][:10]
            if ex_date != current_date:
                current_date = ex_date
                lines.append(f"\n<b>{ex_date}</b>")

            display = display_map.get(ex["account_name"], ex["account_name"])
            status_icon = {
                "filled": "\u2705", "submitted": "\u23f3",
                "cancelled": "\u274c", "failed": "\u274c",
            }.get(ex["status"], "\u2753")
            qty_str = f"{ex['filled_qty']}x" if ex['filled_qty'] else "0x"
            price_str = f"@ ${ex['avg_price']:.2f}" if ex['avg_price'] else ""
            time_str = ex["created_at"][11:16] if len(ex["created_at"]) > 16 else ""
            error_str = f" \u2014 {ex['error']}" if ex.get("error") else ""
            lines.append(
                f"  {status_icon} {time_str} <b>{display}</b>: "
                f"{ex['signal_action']} {qty_str} "
                f"<code>{ex['ticker']}</code> {price_str} "
                f"[{ex['status']}]{error_str}"
            )

        return "\n".join(lines)

    async def _on_cancel_order(self, account_name: str, order_id: int) -> str:
        """Cancel a specific order."""
        connector = self.executor.connectors.get(account_name)
        if not connector:
            return f"\u274c Account {account_name} not connected"
        display = next(
            (a.display_name for a in self.config.accounts if a.name == account_name),
            account_name,
        )
        found = await connector.cancel_order(order_id)
        if found:
            return f"\u2705 {display}: order #{order_id} cancel requested"
        return f"\u274c {display}: order #{order_id} not found"

    async def _on_cancel_all_orders(self) -> str:
        """Cancel all open orders on all accounts."""
        if not self.executor.connectors:
            return "No IBKR accounts connected"
        results = []
        for name, connector in self.executor.connectors.items():
            display = next(
                (a.display_name for a in self.config.accounts if a.name == name),
                name,
            )
            cancelled = await connector.cancel_all_orders()
            results.append(f"{display}: cancelled {cancelled} orders")
        return "\n".join(results)

    async def _on_price_requested(self, raw: str) -> str:
        """Quick quote: stock price + option price for held positions of a ticker."""
        ticker = raw.strip().upper()
        if not ticker:
            raise ValueError("Specify a ticker: /price IREN")

        connector = next(
            (c for c in self.executor.connectors.values() if c.is_connected),
            None,
        )
        if not connector:
            raise ValueError("No IBKR connection")

        lines = [f"\U0001f4b5 <b>{ticker} Quote</b>\n"]

        # Stock price
        try:
            stk_data = await asyncio.wait_for(
                connector.get_stock_prices_batch([ticker]), timeout=15,
            )
            stk = stk_data.get(ticker)
            if stk:
                lines.append(
                    f"<b>Stock:</b> ${stk['price']:.2f}"
                    + (f"  (L ${stk['low']:.2f} \u2013 H ${stk['high']:.2f})"
                       if stk['high'] != stk['low'] else "")
                )
            else:
                lines.append("<b>Stock:</b> no data")
        except Exception as e:
            lines.append(f"<b>Stock:</b> error ({e})")

        # Option prices from held positions
        found_positions = False
        for cfg in self.config.accounts:
            c = self.executor.connectors.get(cfg.name)
            if not c:
                continue
            portfolio = await c.get_portfolio()
            matching = [p for p in portfolio if p.contract.symbol == ticker and p.position > 0]
            if not matching:
                continue
            found_positions = True
            display = cfg.display_name or cfg.name

            for item in matching:
                contract = item.contract
                exp = contract.lastTradeDateOrContractMonth or ""
                exp_short = f"{exp[4:6]}/{exp[0:4]}" if len(exp) >= 6 else exp
                qty = int(item.position)

                # Try live snapshot
                try:
                    detail = await c.get_option_detail(contract)
                except Exception:
                    detail = None

                if detail and (detail["bid"] > 0 or detail["ask"] > 0):
                    mid = detail["mid"]
                    lines.append(
                        f"\n<b>{display}</b> {contract.strike}{contract.right} "
                        f"exp {exp_short} ({qty}x)"
                    )
                    lines.append(
                        f"  Bid ${detail['bid']:.2f} | Ask ${detail['ask']:.2f} | "
                        f"Mid ${mid:.2f}"
                    )
                    spread_pct = detail.get("spread_pct", 0)
                    lines.append(f"  Spread: ${detail['spread']:.2f} ({spread_pct:.1f}%)")
                    value = mid * qty * 100
                    lines.append(f"  Value: ${value:,.0f}")
                else:
                    # Cached fallback
                    cached_price = item.marketPrice
                    if cached_price and not math.isnan(cached_price):
                        lines.append(
                            f"\n<b>{display}</b> {contract.strike}{contract.right} "
                            f"exp {exp_short} ({qty}x)"
                        )
                        lines.append(f"  Price: ${cached_price:.2f} (cached)")
                        lines.append(f"  Value: ${item.marketValue:,.0f}")
                    else:
                        lines.append(
                            f"\n<b>{display}</b> {contract.strike}{contract.right} "
                            f"exp {exp_short} ({qty}x) — no price data"
                        )

        if not found_positions:
            lines.append("\n<i>No open positions for this ticker</i>")

        return "\n".join(lines)

    async def _on_gateway_status(self, account_name: str, connected: bool) -> None:
        """Called when an IB Gateway disconnects or reconnects."""
        if connected:
            text = f"IB Gateway <b>{account_name}</b> reconnected"
            await self.db.log_audit("gateway_reconnected", ticker=account_name)
        else:
            text = f"IB Gateway <b>{account_name}</b> disconnected — all retries exhausted"
            await self.db.log_audit("gateway_disconnected", ticker=account_name,
                                    detail="All reconnect attempts failed")
        logger.warning(text)
        if self.config.bot_token:
            await self.bot.send_notification(text)
