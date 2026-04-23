"""Trade executor — connects to IBKR gateways and places orders.

Supports:
- LEAPS options discovery (2028+ expiry, deep ITM calls)
- Percentage-based position sizing from NLV
- Buy/sell/roll execution with limit orders
- Emergency flatten-all (kill switch)
"""

import asyncio
import hashlib
import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime

from .config import AccountConfig, Config
from .models import TradeSignal

logger = logging.getLogger(__name__)

# Minimum days to expiry to qualify as a LEAPS contract
LEAPS_MIN_DTE = 365
# Timeout for IBKR API calls (seconds)
IBKR_TIMEOUT = 30


@dataclass
class ExecutionResult:
    """Result of a trade execution attempt."""

    account_name: str
    success: bool
    order_id: int | None = None
    filled_qty: int = 0
    avg_price: float = 0.0
    error: str | None = None


RECONNECT_MAX_RETRIES = 5
RECONNECT_BASE_DELAY = 2  # seconds, doubles each retry


class IBKRConnector:
    """Manages connection to a single IB Gateway instance."""

    def __init__(self, account: AccountConfig, on_disconnect=None, on_fill=None):
        self.account = account
        self._ib = None
        self._on_disconnect = on_disconnect  # async callback for notifications
        self._on_fill = on_fill  # async callback(account_name, fill_info)
        self._reconnecting = False
        self._pnl_singles: dict[int, object] = {}  # conId -> PnLSingle (live-updated)
        self._pnl_account: object | None = None  # PnL (live-updated)
        self._reconnect_task: asyncio.Task | None = None

    @property
    def _client_id(self) -> int:
        """Deterministic client ID derived from account name (stable across restarts)."""
        digest = hashlib.sha256(self.account.name.encode()).digest()
        return int.from_bytes(digest[:4], "big") % 900 + 100

    async def connect(self) -> None:
        """Connect to IB Gateway."""
        from ib_async import IB

        self._ib = IB()
        await asyncio.wait_for(
            self._ib.connectAsync(
                host=self.account.gateway_host,
                port=self.account.gateway_port,
                clientId=self._client_id,
            ),
            timeout=IBKR_TIMEOUT,
        )
        # Register handler only after successful connect to avoid stacking
        self._ib.disconnectedEvent += self._handle_disconnect
        accounts = self._ib.managedAccounts()
        logger.info(f"Connected to IBKR [{self.account.name}]: {accounts}")

        # Auto-detect margin account: Cushion field only exists on margin accounts
        acct_type = ""
        has_margin = False
        for item in self._ib.accountValues():
            if item.tag == "AccountType":
                acct_type = item.value
            if item.tag == "Cushion":
                has_margin = True
        self.account.is_margin_account = has_margin
        logger.info(
            f"[{self.account.name}] Account type: {acct_type} "
            f"(margin={'yes' if has_margin else 'no'})"
        )

    def _handle_disconnect(self) -> None:
        """Called by ib_async when connection drops — triggers reconnect."""
        if self._reconnecting:
            return
        self._reconnecting = True
        logger.warning(f"[{self.account.name}] IB Gateway disconnected — will attempt reconnect")
        self._reconnect_task = asyncio.ensure_future(self._reconnect())

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        try:
            for attempt in range(1, RECONNECT_MAX_RETRIES + 1):
                delay = RECONNECT_BASE_DELAY * (2 ** (attempt - 1))
                logger.info(f"[{self.account.name}] Reconnect attempt {attempt}/{RECONNECT_MAX_RETRIES} in {delay}s...")
                await asyncio.sleep(delay)
                try:
                    await self.connect()
                    # Old PnL objects are tied to dead IB instance — clear so
                    # subscribe_pnl() creates fresh ones on the new connection.
                    self._pnl_account = None
                    self._pnl_singles.clear()
                    await self.subscribe_pnl()
                    logger.info(f"[{self.account.name}] Reconnected successfully")
                    if self._on_disconnect:
                        await self._on_disconnect(self.account.name, connected=True)
                    return
                except Exception as e:
                    logger.error(f"[{self.account.name}] Reconnect attempt {attempt} failed: {e}")

            logger.error(f"[{self.account.name}] All reconnect attempts exhausted")
            if self._on_disconnect:
                await self._on_disconnect(self.account.name, connected=False)
        finally:
            self._reconnecting = False

    async def disconnect(self) -> None:
        """Disconnect from IB Gateway."""
        if self._ib:
            self._cancel_pnl_subscriptions()
            self._ib.disconnectedEvent -= self._handle_disconnect
            if self._ib.isConnected():
                self._ib.disconnect()
            logger.info(f"Disconnected from IBKR [{self.account.name}]")

    @property
    def is_connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    async def qualify_contracts(self, *contracts, timeout: float = 15):
        """Qualify one or more contracts via IBKR."""
        return await asyncio.wait_for(
            self._ib.qualifyContractsAsync(*contracts), timeout=timeout,
        )

    async def get_option_chain_params(self, symbol: str, sec_type: str, con_id: int, timeout: float = 15):
        """Get option chain parameters for a symbol."""
        return await asyncio.wait_for(
            self._ib.reqSecDefOptParamsAsync(symbol, "", sec_type, con_id),
            timeout=timeout,
        )

    def managed_accounts(self) -> list[str]:
        """Return the list of managed account IDs."""
        return self._ib.managedAccounts() if self._ib else []

    def req_account_updates(self, subscribe: bool, account: str) -> None:
        """Subscribe/unsubscribe to account updates."""
        if self._ib:
            self._ib.reqAccountUpdates(subscribe, account)

    def get_trades(self) -> list:
        """Return current trade objects."""
        return self._ib.trades() if self._ib else []

    async def get_nlv(self) -> float:
        """Get net liquidation value for this account (first available currency)."""
        if not self._ib:
            return 0.0
        for item in self._ib.accountValues():
            if item.tag == "NetLiquidation" and item.currency:
                return float(item.value)
        all_tags = {item.tag for item in self._ib.accountValues()}
        logger.warning(f"[{self.account.name}] No NetLiquidation found. "
                       f"Available tags ({len(all_tags)}): {sorted(all_tags)[:10]}")
        return 0.0

    async def get_nlv_by_currency(self) -> dict[str, float]:
        """Get NLV in all available currencies (e.g. EUR, USD)."""
        if not self._ib:
            return {}
        return {
            item.currency: float(item.value)
            for item in self._ib.accountValues()
            if item.tag == "NetLiquidation" and item.currency
        }

    async def get_exchange_rate(self, currency: str = "USD") -> float:
        """Get exchange rate from IBKR: how many base-currency units per 1 of `currency`.

        For a EUR-based account, get_exchange_rate("USD") returns ~0.88 (1 USD = 0.88 EUR).
        """
        if not self._ib:
            return 1.0
        for item in self._ib.accountValues():
            if item.tag == "ExchangeRate" and item.currency == currency:
                return float(item.value)
        return 1.0

    async def get_available_funds(self, currency: str = "USD") -> float:
        """Get available funds for a currency, falling back to NLV * 0.3."""
        if not self._ib:
            return 0.0
        for item in self._ib.accountValues():
            if item.tag == "AvailableFunds" and item.currency == currency:
                return float(item.value)
        # Conservative fallback
        nlv = await self.get_nlv()
        return nlv * 0.3

    async def get_cash_balances(self) -> dict[str, float]:
        """Get cash balances per currency from IBKR accountValues."""
        if not self._ib:
            return {}
        result = {}
        for item in self._ib.accountValues():
            if item.tag == "CashBalance" and item.currency and item.currency != "BASE":
                val = float(item.value)
                if abs(val) > 0.01:
                    result[item.currency] = val
        return result

    async def get_margin_used(self) -> float:
        """Get USD margin in use. Negative USD cash = margin borrowed."""
        balances = await self.get_cash_balances()
        usd_cash = balances.get("USD", 0.0)
        return max(0.0, -usd_cash)

    async def get_positions(self) -> list:
        """Get current positions."""
        if not self._ib:
            return []
        return self._ib.positions()

    async def get_portfolio(self) -> list:
        """Get portfolio items with market data (price, value, PnL)."""
        if not self._ib:
            return []
        return self._ib.portfolio()

    async def subscribe_pnl(self) -> None:
        """Subscribe to account and per-position P&L — kept alive for instant reads.

        Call once after connect + portfolio is populated.  Objects are mutated
        in-place by ib_async callbacks, so subsequent reads are instant.
        """
        if not self._ib:
            return
        accounts = self._ib.managedAccounts()
        if not accounts:
            return
        account = accounts[0]

        # Account-level P&L (subscribe once, stays live)
        if not self._pnl_account:
            self._pnl_account = self._ib.reqPnL(account)

        # Per-position P&L
        portfolio = self._ib.portfolio()
        subscribed_ids = set(self._pnl_singles.keys())
        current_ids = {item.contract.conId for item in portfolio}

        # Subscribe to new positions
        for item in portfolio:
            con_id = item.contract.conId
            if con_id not in subscribed_ids:
                try:
                    self._pnl_singles[con_id] = self._ib.reqPnLSingle(account, "", con_id)
                except AssertionError:
                    pass  # Already subscribed (duplicate conId)

        # Cancel removed positions
        for con_id in subscribed_ids - current_ids:
            self._ib.cancelPnLSingle(account, "", con_id)
            self._pnl_singles.pop(con_id, None)

        # Wait for initial data to arrive (first call only)
        if self._pnl_singles:
            for _ in range(30):  # Up to 3 seconds
                all_ready = all(
                    not math.isnan(p.dailyPnL)
                    for p in self._pnl_singles.values()
                )
                if all_ready:
                    break
                await asyncio.sleep(0.1)

    def _cancel_pnl_subscriptions(self) -> None:
        """Cancel all P&L subscriptions."""
        if not self._ib:
            return
        accounts = self._ib.managedAccounts()
        if not accounts:
            return
        account = accounts[0]
        if self._pnl_account:
            self._ib.cancelPnL(account)
            self._pnl_account = None
        for con_id in list(self._pnl_singles):
            self._ib.cancelPnLSingle(account, "", con_id)
        self._pnl_singles.clear()

    async def get_daily_pnl(self) -> dict:
        """Get account-level daily P&L from live subscription.

        Returns dict with keys: dailyPnL, unrealizedPnL, realizedPnL.
        """
        if not self._pnl_account:
            return {"dailyPnL": 0.0, "unrealizedPnL": 0.0, "realizedPnL": 0.0}
        pnl = self._pnl_account
        return {
            "dailyPnL": 0.0 if math.isnan(pnl.dailyPnL) else pnl.dailyPnL,
            "unrealizedPnL": 0.0 if math.isnan(pnl.unrealizedPnL) else pnl.unrealizedPnL,
            "realizedPnL": 0.0 if math.isnan(pnl.realizedPnL) else pnl.realizedPnL,
        }

    async def get_positions_daily_pnl(self) -> dict[int, float]:
        """Get per-position daily P&L from live subscriptions.

        Returns dict of {conId: dailyPnL}.  Reads are instant since
        objects are kept alive and mutated in-place by ib_async.
        """
        result = {}
        for con_id, pnl_single in self._pnl_singles.items():
            daily = pnl_single.dailyPnL
            result[con_id] = 0.0 if math.isnan(daily) else daily
        return result

    async def get_current_price(self, symbol: str) -> float:
        """Get the current market price for a stock via snapshot, with delayed fallback."""
        from ib_async import Stock

        contract = Stock(symbol, "SMART", "USD")
        contracts = await asyncio.wait_for(
            self._ib.qualifyContractsAsync(contract), timeout=IBKR_TIMEOUT
        )
        if not contracts:
            raise ValueError(f"Could not qualify stock contract for {symbol}")

        # Try snapshot first (real-time)
        self._ib.reqMktData(contracts[0], "", True, False)
        try:
            await asyncio.wait_for(self._ib.updateEvent, timeout=5)
        except TimeoutError:
            pass
        ticker = self._ib.ticker(contracts[0])
        self._ib.cancelMktData(contracts[0])

        price = ticker.marketPrice()
        if math.isnan(price):
            price = ticker.close

        # Fallback: delayed market data (type 3)
        if not price or math.isnan(price):
            logger.warning("[%s] Falling back to delayed market data", symbol)
            self._ib.reqMarketDataType(3)
            try:
                self._ib.reqMktData(contracts[0], "", False, False)
                await asyncio.sleep(4)
                ticker = self._ib.ticker(contracts[0])
                self._ib.cancelMktData(contracts[0])
                price = ticker.marketPrice()
                if math.isnan(price):
                    price = ticker.last or ticker.close
            finally:
                self._ib.reqMarketDataType(1)

        if not price or math.isnan(price):
            raise ValueError(f"No price data for {symbol}")
        return price

    async def get_stock_prices_batch(
        self, symbols: list[str],
    ) -> dict[str, dict[str, float]]:
        """Get current price/high/low for multiple stocks in one batch.

        Returns ``{symbol: {"price": …, "high": …, "low": …}}``.
        """
        from ib_async import Stock

        if not symbols or not self._ib:
            return {}

        contracts = [Stock(s, "SMART", "USD") for s in symbols]
        try:
            qualified = await asyncio.wait_for(
                self._ib.qualifyContractsAsync(*contracts), timeout=IBKR_TIMEOUT
            )
        except Exception:
            return {}

        if not qualified:
            return {}

        # Fall back to delayed data if no real-time subscription
        # snapshot=False because delayed data doesn't support snapshots
        self._ib.reqMarketDataType(3)
        try:
            for c in qualified:
                self._ib.reqMktData(c, "", False, False)

            await asyncio.sleep(5)

            result = {}
            for c in qualified:
                t = self._ib.ticker(c)
                data = self._extract_ticker_data(t)
                if data:
                    result[c.symbol] = data
                else:
                    logger.warning("No price data for stock %s: %s", c.symbol, t)
                self._ib.cancelMktData(c)
        finally:
            self._ib.reqMarketDataType(1)

        return result

    @staticmethod
    def _extract_ticker_data(t) -> dict[str, float] | None:
        """Read price/high/low from ticker, falling back to delayed fields."""
        def _valid(v):
            return v is not None and v == v and v > 0

        price = t.marketPrice()
        if not _valid(price):
            price = getattr(t, "delayedLast", float("nan"))
        if not _valid(price):
            price = t.close
        if not _valid(price):
            price = getattr(t, "delayedClose", float("nan"))
        if not _valid(price):
            return None

        high = t.high
        if not _valid(high):
            high = getattr(t, "delayedHigh", float("nan"))
        if not _valid(high):
            high = price

        low = t.low
        if not _valid(low):
            low = getattr(t, "delayedLow", float("nan"))
        if not _valid(low):
            low = price

        return {"price": price, "high": high, "low": low}

    async def get_option_data_batch(
        self, contracts: list,
    ) -> dict[int, dict[str, float]]:
        """Get current price/high/low for option contracts.

        Returns ``{conId: {"price": …, "high": …, "low": …}}``.
        """
        if not contracts or not self._ib:
            return {}

        # Ensure exchange is set (portfolio contracts may lack it)
        for c in contracts:
            if not c.exchange:
                c.exchange = "SMART"

        self._ib.reqMarketDataType(3)
        try:
            for c in contracts:
                self._ib.reqMktData(c, "", False, False)

            await asyncio.sleep(5)

            result = {}
            for c in contracts:
                t = self._ib.ticker(c)
                data = self._extract_ticker_data(t)
                if data:
                    result[c.conId] = data
                self._ib.cancelMktData(c)
        finally:
            self._ib.reqMarketDataType(1)

        return result

    async def find_leaps_contract(self, symbol: str, right: str = "C"):
        """Find a suitable LEAPS option contract for a symbol.

        Strategy: furthest expiration, deepest ITM strike available.
        Selects the furthest expiry with the deepest ITM strike.
        """
        from ib_async import Option, Stock

        # Qualify the underlying stock
        stock = Stock(symbol, "SMART", "USD")
        qualified = await asyncio.wait_for(
            self._ib.qualifyContractsAsync(stock), timeout=IBKR_TIMEOUT
        )
        if not qualified:
            raise ValueError(f"Could not qualify {symbol}")

        # Get option chain parameters
        chains = await asyncio.wait_for(
            self._ib.reqSecDefOptParamsAsync(
                qualified[0].symbol,
                "",
                qualified[0].secType,
                qualified[0].conId,
            ),
            timeout=IBKR_TIMEOUT,
        )
        if not chains:
            raise ValueError(f"No option chains for {symbol}")

        # Pick the SMART exchange chain (or first available)
        chain = next((c for c in chains if c.exchange == "SMART"), chains[0])

        # Filter to LEAPS expirations (>1 year out)
        now = datetime.now(UTC)
        leaps_expirations = []
        for exp_str in sorted(chain.expirations):
            exp_date = datetime.strptime(exp_str, "%Y%m%d").replace(tzinfo=UTC)
            dte = (exp_date - now).days
            if dte >= LEAPS_MIN_DTE:
                leaps_expirations.append(exp_str)

        if not leaps_expirations:
            raise ValueError(f"No LEAPS expirations (>{LEAPS_MIN_DTE} DTE) for {symbol}")

        # Use the furthest expiration
        target_exp = leaps_expirations[-1]

        # Get current price to pick a deep ITM strike
        current_price = await self.get_current_price(symbol)

        # For calls: pick the highest strike that is still ITM (strike < current_price)
        # For puts: pick the lowest strike that is still ITM (strike > current_price)
        available_strikes = sorted(chain.strikes)

        if right == "C":
            itm_strikes = [s for s in available_strikes if s <= current_price]
            if not itm_strikes:
                # No ITM strikes — pick the lowest available (closest to ATM)
                target_strike = available_strikes[0]
            else:
                # Deepest ITM = lowest strike for calls
                target_strike = itm_strikes[0]
        else:
            itm_strikes = [s for s in available_strikes if s >= current_price]
            if not itm_strikes:
                target_strike = available_strikes[-1]
            else:
                target_strike = itm_strikes[-1]

        # Build and qualify the option contract
        option = Option(symbol, target_exp, target_strike, right, "SMART")
        qualified_opts = await asyncio.wait_for(
            self._ib.qualifyContractsAsync(option), timeout=IBKR_TIMEOUT
        )
        if not qualified_opts:
            raise ValueError(
                f"Could not qualify {symbol} {target_exp} {target_strike}{right}"
            )

        logger.info(
            f"Found LEAPS: {symbol} {target_exp} {target_strike}{right} "
            f"(current price=${current_price:.2f})"
        )
        return qualified_opts[0]

    async def get_option_price(self, contract) -> float:
        """Get current market price for an option contract, with delayed fallback."""
        if not contract.exchange:
            contract.exchange = "SMART"

        # Try snapshot first (real-time)
        self._ib.reqMktData(contract, "", True, False)
        try:
            await asyncio.wait_for(self._ib.updateEvent, timeout=5)
        except TimeoutError:
            pass
        ticker = self._ib.ticker(contract)
        self._ib.cancelMktData(contract)

        price = ticker.marketPrice()
        if math.isnan(price):
            price = ticker.close

        # Fallback: delayed market data (type 3)
        if not price or math.isnan(price):
            logger.warning("[%s] Falling back to delayed market data", contract.localSymbol)
            self._ib.reqMarketDataType(3)
            try:
                self._ib.reqMktData(contract, "", False, False)
                await asyncio.sleep(4)
                ticker = self._ib.ticker(contract)
                self._ib.cancelMktData(contract)
                price = ticker.marketPrice()
                if math.isnan(price):
                    price = ticker.last or ticker.close
            finally:
                self._ib.reqMarketDataType(1)

        if not price or math.isnan(price):
            raise ValueError(f"No price data for option {contract.localSymbol}")
        return price

    async def get_option_detail(self, contract) -> dict:
        """Get full option market data: bid/ask/last/Greeks via snapshot."""
        if not contract.exchange:
            contract.exchange = "SMART"
        self._ib.reqMktData(contract, "106", True, False)  # 106 = implied vol + greeks
        await asyncio.wait_for(self._ib.updateEvent, timeout=IBKR_TIMEOUT)
        ticker = self._ib.ticker(contract)
        self._ib.cancelMktData(contract)

        bid = ticker.bid if ticker.bid == ticker.bid else 0.0
        ask = ticker.ask if ticker.ask == ticker.ask else 0.0
        last = ticker.last if ticker.last == ticker.last else 0.0
        close = ticker.close if ticker.close == ticker.close else 0.0
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else last or close

        greeks = ticker.modelGreeks or ticker.lastGreeks
        delta = greeks.delta if greeks and greeks.delta == greeks.delta else None
        gamma = greeks.gamma if greeks and greeks.gamma == greeks.gamma else None
        theta = greeks.theta if greeks and greeks.theta == greeks.theta else None
        iv = greeks.impliedVol if greeks and greeks.impliedVol == greeks.impliedVol else None

        return {
            "bid": bid,
            "ask": ask,
            "last": last,
            "mid": mid,
            "close": close,
            "spread": ask - bid if bid > 0 and ask > 0 else 0.0,
            "spread_pct": ((ask - bid) / mid * 100) if mid > 0 and bid > 0 else 0.0,
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "iv": iv,
        }

    async def place_order(self, contract, order) -> int:
        """Place an order and return the order ID."""
        # Ensure exchange is set (positions contracts may lack it)
        if not contract.exchange:
            contract.exchange = "SMART"
        trade = self._ib.placeOrder(contract, order)
        logger.info(
            f"[{self.account.name}] Order placed: {order.action} {order.totalQuantity} "
            f"{contract.localSymbol} @ {order.orderType} {getattr(order, 'lmtPrice', 'MKT')}"
        )
        # Monitor all status changes (submitted, filled, rejected, cancelled)
        trade.statusEvent += lambda t: self._handle_status(t)
        return trade.order.orderId

    def _handle_status(self, trade) -> None:
        """Called on any order status change — dispatches to on_fill callback."""
        if not self._on_fill:
            return
        status = trade.orderStatus.status
        # Skip noisy intermediate states
        if status in ("PendingSubmit", "PreSubmitted"):
            return

        info = {
            "symbol": trade.contract.symbol,
            "local_symbol": trade.contract.localSymbol,
            "action": trade.order.action,
            "qty": int(trade.order.totalQuantity),
            "avg_price": trade.orderStatus.avgFillPrice,
            "order_id": trade.order.orderId,
            "filled": int(trade.orderStatus.filled),
            "remaining": int(trade.orderStatus.remaining),
        }

        if status == "Submitted":
            info["event"] = "submitted"
            logger.info(
                f"[{self.account.name}] SUBMITTED: {info['action']} {info['qty']}x "
                f"{info['local_symbol']}"
            )
        elif status == "Filled":
            info["event"] = "filled"
            info["qty"] = int(trade.orderStatus.filled)
            logger.info(
                f"[{self.account.name}] FILLED: {info['action']} {info['qty']}x "
                f"{info['local_symbol']} @ ${info['avg_price']:.2f}"
            )
        elif status in ("Cancelled", "ApiCancelled"):
            info["event"] = "cancelled"
            logger.info(
                f"[{self.account.name}] CANCELLED: {info['action']} {info['qty']}x "
                f"{info['local_symbol']}"
            )
        elif status == "Inactive":
            info["event"] = "rejected"
            # Grab error message from trade log
            error_msg = ""
            if trade.log:
                error_msg = trade.log[-1].message
            info["error"] = error_msg
            logger.warning(
                f"[{self.account.name}] REJECTED: {info['action']} {info['qty']}x "
                f"{info['local_symbol']} — {error_msg}"
            )
        else:
            return  # Ignore unknown states

        asyncio.ensure_future(self._safe_dispatch(self.account.name, info))

    async def _safe_dispatch(self, account_name: str, info: dict) -> None:
        """Dispatch order event callback with exception logging."""
        try:
            await self._on_fill(account_name, info)
        except Exception:
            logger.error(
                f"[{account_name}] Order event dispatch failed for "
                f"{info.get('event', '?')} {info.get('local_symbol', '?')}",
                exc_info=True,
            )

    def get_open_orders(self) -> list[dict]:
        """Return all open/working orders as dicts."""
        if not self._ib or not self._ib.isConnected():
            return []
        result = []
        for trade in self._ib.openTrades():
            o = trade.order
            c = trade.contract
            s = trade.orderStatus
            result.append({
                "order_id": o.orderId,
                "symbol": c.symbol,
                "local_symbol": c.localSymbol or c.symbol,
                "action": o.action,
                "qty": int(o.totalQuantity),
                "order_type": o.orderType,
                "limit_price": getattr(o, "lmtPrice", 0.0),
                "status": s.status,
                "filled": int(s.filled),
                "remaining": int(s.remaining),
            })
        return result

    async def cancel_order(self, order_id: int) -> bool:
        """Cancel a specific order by ID. Returns True if found."""
        if not self._ib or not self._ib.isConnected():
            return False
        for trade in self._ib.openTrades():
            if trade.order.orderId == order_id:
                self._ib.cancelOrder(trade.order)
                return True
        return False

    async def cancel_all_orders(self) -> int:
        """Cancel all open/pending orders. Returns count of orders cancelled."""
        if not self._ib or not self._ib.isConnected():
            logger.warning(f"[{self.account.name}] Cannot cancel orders — not connected")
            return 0

        open_trades = self._ib.openTrades()
        cancelled = 0

        for trade in open_trades:
            try:
                self._ib.cancelOrder(trade.order)
                cancelled += 1
                logger.info(
                    f"[{self.account.name}] Cancelled: {trade.order.action} "
                    f"{trade.order.totalQuantity} {trade.contract.symbol}"
                )
            except Exception as e:
                logger.error(
                    f"[{self.account.name}] Failed to cancel order "
                    f"{trade.order.orderId}: {e}"
                )

        if cancelled == 0:
            logger.info(f"[{self.account.name}] No open orders to cancel")

        return cancelled


class TradeExecutor:
    """Executes trades across multiple IBKR accounts."""

    def __init__(self, config: Config, on_disconnect=None, on_fill=None):
        self.config = config
        self.connectors: dict[str, IBKRConnector] = {}
        self._on_disconnect = on_disconnect  # async callback(account_name, connected)
        self._on_fill = on_fill  # async callback(account_name, fill_info)

    async def connect_all(self) -> None:
        """Connect to all configured IB Gateways with retry.

        Always registers connectors so the app starts regardless.
        Failed connections retry in the background until the gateway is ready.
        """
        for account in self.config.accounts:
            connector = IBKRConnector(account, on_disconnect=self._on_disconnect, on_fill=self._on_fill)
            self.connectors[account.name] = connector

            connected = False
            for attempt in range(1, RECONNECT_MAX_RETRIES + 1):
                try:
                    await connector.connect()
                    connected = True
                    break
                except Exception as e:
                    delay = RECONNECT_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        f"[{account.name}] Connect attempt {attempt}/{RECONNECT_MAX_RETRIES} "
                        f"failed: {e} — retrying in {delay}s"
                    )
                    await asyncio.sleep(delay)

            if not connected:
                logger.error(
                    f"[{account.name}] Initial connection failed after {RECONNECT_MAX_RETRIES} "
                    f"attempts — will keep retrying in background"
                )
                # Start background reconnect loop so it connects when gateway is ready
                connector._reconnecting = True
                connector._reconnect_task = asyncio.ensure_future(
                    self._background_connect(connector)
                )

    async def _background_connect(self, connector: IBKRConnector) -> None:
        """Keep trying to connect indefinitely until gateway becomes available."""
        account_name = connector.account.name
        attempt = 0
        try:
            while True:
                attempt += 1
                delay = min(60, RECONNECT_BASE_DELAY * (2 ** min(attempt - 1, 5)))
                await asyncio.sleep(delay)
                try:
                    await connector.connect()
                    logger.info(f"[{account_name}] Background connect succeeded on attempt {attempt}")
                    if self._on_disconnect:
                        await self._on_disconnect(account_name, connected=True)
                    return
                except Exception as e:
                    if attempt % 10 == 0:
                        logger.warning(f"[{account_name}] Still trying to connect (attempt {attempt}): {e}")
        finally:
            connector._reconnecting = False

    async def disconnect_all(self) -> None:
        """Disconnect from all gateways."""
        for connector in self.connectors.values():
            await connector.disconnect()

    async def execute(
        self, signal: TradeSignal, exclude_accounts: set[str] | None = None,
    ) -> list[ExecutionResult]:
        """Execute a trade signal across all connected accounts."""
        results = []

        for name, connector in self.connectors.items():
            if exclude_accounts and name in exclude_accounts:
                results.append(ExecutionResult(
                    account_name=name,
                    success=False,
                    error="Skipped — position limit breach",
                ))
                continue
            if not connector.is_connected:
                results.append(ExecutionResult(
                    account_name=name,
                    success=False,
                    error="Gateway disconnected — reconnecting",
                ))
                continue
            account_cfg = next(a for a in self.config.accounts if a.name == name)
            try:
                result = await self._execute_for_account(signal, connector, account_cfg)
                results.append(result)
            except Exception as e:
                logger.error(f"Execution failed for {name}: {e}", exc_info=True)
                results.append(ExecutionResult(
                    account_name=name,
                    success=False,
                    error=str(e),
                ))

        return results

    async def _execute_for_account(
        self,
        signal: TradeSignal,
        connector: IBKRConnector,
        account: AccountConfig,
    ) -> ExecutionResult:
        """Execute a trade for a single account with proper sizing."""
        nlv = await connector.get_nlv()
        if nlv <= 0:
            return ExecutionResult(
                account_name=account.name,
                success=False,
                error="Could not retrieve NLV",
            )

        if signal.action == "BUY":
            return await self._execute_buy(signal, connector, account, nlv)
        elif signal.action == "SELL":
            return await self._execute_sell(signal, connector, account, nlv)
        elif signal.action == "ROLL":
            return await self._execute_roll(signal, connector, account, nlv)
        else:
            return ExecutionResult(
                account_name=account.name,
                success=False,
                error=f"Unknown action: {signal.action}",
            )

    async def _execute_buy(
        self,
        signal: TradeSignal,
        connector: IBKRConnector,
        account: AccountConfig,
        nlv: float,
    ) -> ExecutionResult:
        """Execute a buy order — find LEAPS, size from NLV, place order."""
        from ib_async import LimitOrder, MarketOrder

        # Calculate target allocation — use USD NLV since options are priced in USD
        nlv_all = await connector.get_nlv_by_currency()
        nlv_usd = nlv_all.get("USD", 0.0)
        if nlv_usd <= 0:
            # Convert EUR NLV to USD as fallback
            eur_per_usd = await connector.get_exchange_rate("USD")
            nlv_eur = nlv_all.get("EUR", nlv)
            nlv_usd = nlv_eur / eur_per_usd if eur_per_usd > 0 else nlv

        target_pct = signal.target_weight_pct or 5.0
        target_pct = min(target_pct, account.max_position_pct)
        target_value = nlv_usd * (target_pct / 100)

        logger.info(
            f"[{account.name}] BUY ${signal.ticker}: NLV_USD=${nlv_usd:.0f}, "
            f"target={target_pct}%, value=${target_value:.0f}"
        )

        # Find LEAPS contract
        contract = await connector.find_leaps_contract(signal.ticker)

        # Get option price
        option_price = await connector.get_option_price(contract)
        cost_per_contract = option_price * 100  # Options multiplier

        if cost_per_contract <= 0:
            return ExecutionResult(
                account_name=account.name,
                success=False,
                error=f"Invalid option price: {option_price}",
            )

        # Calculate quantity — round up when margin is available to prefer
        # slight over-deployment over leaving idle cash
        margin_active = account.margin_mode in ("soft", "hard") and account.is_margin_account
        if margin_active:
            qty = max(1, math.ceil(target_value / cost_per_contract))
        else:
            qty = max(1, int(target_value / cost_per_contract))

        # Enforce margin limit: don't exceed cash + max_margin_usd (0 = no limit)
        if margin_active and account.max_margin_usd > 0:
            available = await connector.get_available_funds("USD")
            budget = available + account.max_margin_usd
            max_qty_margin = math.ceil(budget / cost_per_contract)
            if max_qty_margin < 1:
                return ExecutionResult(
                    account_name=account.name,
                    success=False,
                    error=(
                        f"Insufficient funds: need ${cost_per_contract:,.0f}/contract, "
                        f"have ${available:,.0f} + ${account.max_margin_usd:,.0f} margin"
                    ),
                )
            if max_qty_margin < qty:
                logger.info(
                    f"[{account.name}] Margin cap: {qty} → {max_qty_margin} "
                    f"(cash=${available:,.0f}, margin=${account.max_margin_usd:,.0f})"
                )
                qty = max_qty_margin

        # Enforce deviation: clamp qty to stay within max_deviation_pct
        actual_value = qty * cost_per_contract
        actual_pct = (actual_value / nlv_usd) * 100
        deviation = abs(actual_pct - target_pct)

        if deviation > self.config.trading.max_deviation_pct:
            max_value = nlv_usd * ((target_pct + self.config.trading.max_deviation_pct) / 100)
            clamped_qty = int(max_value / cost_per_contract)
            if clamped_qty < 1:
                return ExecutionResult(
                    account_name=account.name,
                    success=False,
                    error=(
                        f"Single contract costs {actual_pct:.1f}% of NLV "
                        f"(target {target_pct}% ± {self.config.trading.max_deviation_pct}%)"
                    ),
                )
            if clamped_qty < qty:
                logger.info(
                    f"[{account.name}] Clamped qty {qty} → {clamped_qty} "
                    f"to stay within {self.config.trading.max_deviation_pct}% deviation"
                )
                qty = clamped_qty
                actual_value = qty * cost_per_contract
                actual_pct = (actual_value / nlv_usd) * 100

        # Place order (market or limit based on config)
        if self.config.trading.order_type == "MKT":
            order = MarketOrder("BUY", qty)
            price_str = "MKT"
        else:
            offset_pct = self.config.trading.limit_offset_pct / 100
            limit_price = round(option_price * (1 + offset_pct), 2)
            order = LimitOrder("BUY", qty, limit_price)
            price_str = f"${limit_price:.2f}"

        order_id = await connector.place_order(contract, order)

        logger.info(
            f"[{account.name}] Placed BUY {qty}x {contract.localSymbol} "
            f"@ {price_str} (target={target_pct}%, actual={actual_pct:.1f}%)"
        )

        return ExecutionResult(
            account_name=account.name,
            success=True,
            order_id=order_id,
            filled_qty=0,
            avg_price=0.0,
        )

    async def _execute_sell(
        self,
        signal: TradeSignal,
        connector: IBKRConnector,
        account: AccountConfig,
        nlv: float,
    ) -> ExecutionResult:
        """Execute a sell order — find existing position, size the sell, place order."""
        from ib_async import LimitOrder, MarketOrder

        # Find existing position for this ticker
        positions = await connector.get_positions()
        matching = [
            p for p in positions
            if p.contract.symbol == signal.ticker and p.position > 0
        ]

        if not matching:
            return ExecutionResult(
                account_name=account.name,
                success=False,
                error=f"No position found for {signal.ticker}",
            )

        pos = matching[0]
        current_qty = int(pos.position)

        # Determine sell quantity based on amount description
        sell_pct = self._parse_sell_fraction(signal.amount_description)
        sell_qty = min(current_qty, max(1, int(current_qty * sell_pct)))

        logger.info(
            f"[{account.name}] SELL ${signal.ticker}: hold={current_qty}, "
            f"sell={sell_qty} ({sell_pct*100:.0f}%)"
        )

        # Get current price and place order (market or limit)
        option_price = await connector.get_option_price(pos.contract)

        if self.config.trading.order_type == "MKT":
            order = MarketOrder("SELL", sell_qty)
            price_str = "MKT"
        else:
            offset_pct = self.config.trading.limit_offset_pct / 100
            limit_price = round(option_price * (1 - offset_pct), 2)
            order = LimitOrder("SELL", sell_qty, limit_price)
            price_str = f"${limit_price:.2f}"

        order_id = await connector.place_order(pos.contract, order)

        logger.info(
            f"[{account.name}] Placed SELL {sell_qty}x {pos.contract.localSymbol} "
            f"@ {price_str}"
        )

        return ExecutionResult(
            account_name=account.name,
            success=True,
            order_id=order_id,
            filled_qty=0,
            avg_price=0.0,
        )

    async def _execute_roll(
        self,
        signal: TradeSignal,
        connector: IBKRConnector,
        account: AccountConfig,
        nlv: float,
    ) -> ExecutionResult:
        """Execute a roll — close existing position, then open new LEAPS."""
        # First, sell the existing position
        sell_result = await self._execute_sell(signal, connector, account, nlv)
        if not sell_result.success:
            return ExecutionResult(
                account_name=account.name,
                success=False,
                error=f"Roll failed on sell leg: {sell_result.error}",
            )

        # Then buy the new LEAPS (uses related_ticker if present, else same ticker)
        buy_ticker = signal.related_ticker or signal.ticker
        buy_signal = TradeSignal(
            ticker=buy_ticker,
            action="BUY",
            target_weight_pct=signal.target_weight_pct,
            amount_description=signal.amount_description,
            raw_text=signal.raw_text,
            source=signal.source,
        )
        buy_result = await self._execute_buy(buy_signal, connector, account, nlv)
        if not buy_result.success:
            logger.critical(
                "[%s] ROLL PARTIAL FAILURE: sold %s but buy of %s failed: %s. "
                "Manual intervention required!",
                account.name, signal.ticker, buy_ticker, buy_result.error,
            )
            return ExecutionResult(
                account_name=account.name,
                success=False,
                error=f"URGENT: Roll sold {signal.ticker} OK but buy {buy_ticker} "
                      f"failed: {buy_result.error}. Position is FLAT — manual action needed!",
            )

        return ExecutionResult(
            account_name=account.name,
            success=True,
            order_id=buy_result.order_id,
            filled_qty=buy_result.filled_qty,
            avg_price=buy_result.avg_price,
        )

    @staticmethod
    def _parse_sell_fraction(amount_desc: str) -> float:
        """Convert amount description to a sell fraction (0.0-1.0)."""
        if not amount_desc:
            return 0.5  # Default: sell half

        desc = amount_desc.strip().lower()
        presets = {
            "small": 0.25, "quarter": 0.25,
            "third": 1.0 / 3.0,
            "half": 0.5,
            "large": 1.0, "all": 1.0,
        }
        if desc in presets:
            return presets[desc]

        # Percentage like "10%"
        if desc.endswith("%"):
            try:
                pct = float(desc.rstrip("% ")) / 100
                return max(0.0, min(1.0, pct))
            except ValueError:
                pass

        raise ValueError(f"Unrecognized sell amount: '{amount_desc}'")
