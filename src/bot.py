"""Telegram bot for trade confirmation and portfolio management."""

import logging
import time as _time
from collections.abc import Awaitable, Callable
from html import escape

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .models import TradeSignal

logger = logging.getLogger(__name__)

router = Router()

# Module-level state shared between ConfirmationBot and router handlers
_pending: dict[str, dict] = {}  # signal_key -> {"signal_id": int, "signal": TradeSignal, "_created_at": float}
_admin_chat_id: int = 0
_on_confirm: Callable[[int, TradeSignal], Awaitable] | None = None
_on_positions: Callable[[], Awaitable[str]] | None = None
_on_kill: Callable[[], Awaitable[str]] | None = None
_on_portfolio: Callable[[], Awaitable[str]] | None = None
_on_account: Callable[[str], Awaitable[str]] | None = None
_on_signals: Callable[[int], Awaitable[str]] | None = None
_on_health: Callable[[], Awaitable[str]] | None = None
_on_deposits: Callable[[str], Awaitable[str]] | None = None
_on_value: Callable[[str], Awaitable[str]] | None = None
_on_buy_preview: Callable[[str], Awaitable[tuple[str, dict]]] | None = None
_on_buy_execute: Callable[[dict], Awaitable[str]] | None = None
_on_sell_preview: Callable[[str], Awaitable[tuple[str, dict]]] | None = None
_on_sell_execute: Callable[[dict], Awaitable[str]] | None = None
_on_list_positions: Callable[[], Awaitable[list[dict]]] | None = None
_on_option_expiries: Callable[[str], Awaitable[list[dict]]] | None = None
_on_option_strikes: Callable[[str, str, str], Awaitable[dict]] | None = None
_on_info: Callable[[str], Awaitable[str]] | None = None
_on_pause: Callable[[int], Awaitable[str]] | None = None
_on_resume: Callable[[], Awaitable[str]] | None = None
_on_orders: Callable[[], Awaitable[tuple[str, list[dict]]]] | None = None
_on_cancel_order: Callable[[str, int], Awaitable[str]] | None = None
_on_cancel_all: Callable[[], Awaitable[str]] | None = None
_on_price: Callable[[str], Awaitable[str]] | None = None
_on_trades: Callable[[str], Awaitable[str]] | None = None
_pending_orders: dict[str, dict] = {}  # order_key -> order_details (includes _created_at)
_executed_orders: dict[str, float] = {}  # order_key -> timestamp (dedup)
_order_counter: int = 0  # monotonic counter for unique order keys
_PENDING_ORDER_TTL = 600  # 10 minutes
_bot_instance: Bot | None = None
_dashboard_url: str = ""

HELP_TEXT = """
<b>IBKR-Telegram Bot</b>

<b>Portfolio:</b>
/v — Full portfolio: NLV, positions, P&amp;L
/vhelp — Explain /v fields
/price TICKER — Live stock + option quote

<b>Trading:</b>
/buy TICKER PCT PRICE|MKT — Add to position
/sell TICKER all|half|% PRICE|MKT — Reduce/close
/new — Open new position (option chain wizard)
/info — Position details: bid/ask, Greeks, P&amp;L
/orders — View &amp; cancel open orders
/trades — Execution history (today/week)
/kill — Cancel all open orders

<b>Info:</b>
/deposits — Deposit/withdrawal history
/signals — Recent signal history
/status — System health + connectivity
/pending — Pending trade confirmations
/pause — Pause gateways to log into IBKR

/help — Show this message
"""

VHELP_TEXT = """\
\U0001f4ca <b>Portfolio Help</b>

<b>Accounts</b>
\u250c/\u2514 NLV \u2022 Return% \u2022 Today's P&amp;L

<b>Positions</b>
\U0001f7e2/\U0001f7e1/\U0001f534 P&amp;L color: &gt;+2% / \u00b12% / &lt;-2%
<code>TICK</code> 85C 01/28 \u2014 23%
\u2514 Ticker \u2022 Strike+Type \u2022 Expiry \u2022 NLV weight

$12.45 stk \u2502 $13.87 opt \u25b2$0.52
\u2514 Stock price \u2022 Option price + day change

56x (S:16/L:40) \u2022 \u20ac22,194
\u2514 Qty per account \u2022 Market value in EUR

\u25b2\u20ac+1,234 (+5.1%) \u2022 Today \u25b2\u20ac+567
\u2514 Total unrealized \u2022 Today's change

\u26aa <b>Cash</b> \u2014 NLV not in positions
\U0001f504 <b>Refresh</b> reloads \u2022 <b>Day/Total</b> swaps focus"""


class ConfirmationBot:
    """Telegram bot that sends trade signals for user confirmation."""

    def __init__(
        self,
        bot_token: str,
        admin_chat_id: int,
        on_confirm: Callable[[int, TradeSignal], Awaitable] | None = None,
        on_positions: Callable[[], Awaitable[str]] | None = None,
        on_kill: Callable[[], Awaitable[str]] | None = None,
        on_portfolio: Callable[[], Awaitable[str]] | None = None,
        on_account: Callable[[str], Awaitable[str]] | None = None,
        on_signals: Callable[[int], Awaitable[str]] | None = None,
        on_health: Callable[[], Awaitable[str]] | None = None,
        on_deposits: Callable[[str], Awaitable[str]] | None = None,
        on_value: Callable[[str], Awaitable[str]] | None = None,
        on_buy_preview: Callable[[str], Awaitable[tuple[str, dict]]] | None = None,
        on_buy_execute: Callable[[dict], Awaitable[str]] | None = None,
        on_sell_preview: Callable[[str], Awaitable[tuple[str, dict]]] | None = None,
        on_sell_execute: Callable[[dict], Awaitable[str]] | None = None,
        on_list_positions: Callable[[], Awaitable[list[dict]]] | None = None,
        on_option_expiries: Callable[[str], Awaitable[list[dict]]] | None = None,
        on_option_strikes: Callable[[str, str, str], Awaitable[dict]] | None = None,
        on_info: Callable[[str], Awaitable[str]] | None = None,
        on_pause: Callable[[int], Awaitable[str]] | None = None,
        on_resume: Callable[[], Awaitable[str]] | None = None,
        on_orders: Callable[[], Awaitable[tuple[str, list[dict]]]] | None = None,
        on_cancel_order: Callable[[str, int], Awaitable[str]] | None = None,
        on_cancel_all: Callable[[], Awaitable[str]] | None = None,
        on_price: Callable[[str], Awaitable[str]] | None = None,
        on_trades: Callable[[str], Awaitable[str]] | None = None,
        dashboard_url: str = "",
    ):
        global _admin_chat_id, _on_confirm, _on_positions, _on_kill, _bot_instance
        global _on_portfolio, _on_account, _on_signals, _on_health, _on_deposits, _on_value
        global _on_buy_preview, _on_buy_execute, _on_sell_preview, _on_sell_execute
        global _on_list_positions, _on_option_expiries, _on_option_strikes, _on_info
        global _on_pause, _on_resume, _on_orders, _on_cancel_order, _on_cancel_all
        global _on_price, _on_trades, _dashboard_url
        self.bot = Bot(token=bot_token)
        self.dp = Dispatcher()
        self.dp.include_router(router)
        self.admin_chat_id = admin_chat_id
        _admin_chat_id = admin_chat_id
        _on_confirm = on_confirm
        _on_positions = on_positions
        _on_kill = on_kill
        _on_portfolio = on_portfolio
        _on_account = on_account
        _on_signals = on_signals
        _on_health = on_health
        _on_deposits = on_deposits
        _on_value = on_value
        _on_buy_preview = on_buy_preview
        _on_buy_execute = on_buy_execute
        _on_sell_preview = on_sell_preview
        _on_sell_execute = on_sell_execute
        _on_list_positions = on_list_positions
        _on_option_expiries = on_option_expiries
        _on_option_strikes = on_option_strikes
        _on_info = on_info
        _on_pause = on_pause
        _on_resume = on_resume
        _on_orders = on_orders
        _on_cancel_order = on_cancel_order
        _on_cancel_all = on_cancel_all
        _on_price = on_price
        _on_trades = on_trades
        _bot_instance = self.bot
        _dashboard_url = dashboard_url

    @staticmethod
    def get_pending_count() -> int:
        """Return the number of pending trade confirmations."""
        return len(_pending)

    async def send_confirmation(self, signal: TradeSignal, signal_id: int = 0) -> None:
        """Send a trade signal to the admin for confirmation."""
        _cleanup_stale_pending()
        ts = int(signal.timestamp.timestamp()) if signal.timestamp else 0
        signal_key = f"{signal.ticker}_{signal.action}_{ts}_{signal_id}"
        _pending[signal_key] = {"signal_id": signal_id, "signal": signal, "_created_at": _time.monotonic()}

        text = (
            f"\U0001f514 <b>New Trade Signal</b>\n\n"
            f"<b>Ticker:</b> ${escape(signal.ticker)}\n"
            f"<b>Action:</b> {escape(signal.action)}\n"
        )
        if signal.target_weight_pct:
            text += f"<b>Target Weight:</b> {signal.target_weight_pct}%\n"
        if signal.amount_description:
            text += f"<b>Amount:</b> {escape(signal.amount_description)}\n"
        if signal.related_ticker:
            text += f"<b>Related:</b> ${escape(signal.related_ticker)}\n"
        text += f"\n<i>Source: {escape(signal.source)}</i>"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="\u2705 Execute", callback_data=f"exec:{signal_key}"),
                InlineKeyboardButton(text="\u274c Skip", callback_data=f"skip:{signal_key}"),
            ],
        ])

        await self.bot.send_message(
            self.admin_chat_id,
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        logger.info(f"Sent confirmation request for {signal.ticker} {signal.action} (id={signal_id})")

    async def send_notification(self, text: str) -> None:
        """Send a notification message to the admin."""
        await self.bot.send_message(
            self.admin_chat_id,
            text,
            parse_mode="HTML",
        )

    async def start(self) -> None:
        """Start polling for bot updates."""
        logger.info("Starting confirmation bot...")
        from aiogram.types import BotCommand
        await self.bot.set_my_commands([
            BotCommand(command="v", description="Portfolio snapshot with P&L"),
            BotCommand(command="buy", description="Add to existing position"),
            BotCommand(command="sell", description="Reduce/close position"),
            BotCommand(command="new", description="Open new position (chain wizard)"),
            BotCommand(command="info", description="Position details: bid/ask, Greeks"),
            BotCommand(command="price", description="Quick stock + option quote"),
            BotCommand(command="orders", description="View & cancel open orders"),
            BotCommand(command="vhelp", description="Explain /v fields"),
            BotCommand(command="status", description="System health & connectivity"),
            BotCommand(command="signals", description="Recent signal history"),
            BotCommand(command="deposits", description="Deposit/withdrawal history"),
            BotCommand(command="pending", description="Pending trade confirmations"),
            BotCommand(command="kill", description="Cancel all open orders"),
            BotCommand(command="pause", description="Pause gateways to log into IBKR"),
            BotCommand(command="help", description="Show all commands"),
        ])
        await self.dp.start_polling(self.bot)

    async def stop(self) -> None:
        """Stop the bot."""
        await self.bot.session.close()


def _is_admin(user_id: int) -> bool:
    """Check if the user is the authorized admin."""
    if _admin_chat_id == 0:
        return False
    return user_id == _admin_chat_id


def _cleanup_stale_pending() -> None:
    """Remove pending confirmations older than 24 hours."""
    now = _time.monotonic()
    stale = [k for k, v in _pending.items() if isinstance(v, dict) and now - v.get("_created_at", now) > 86400]
    for k in stale:
        _pending.pop(k, None)


def _next_order_id() -> int:
    """Generate a unique monotonic order ID."""
    global _order_counter
    _order_counter += 1
    return _order_counter


def _store_pending_order(order_key: str, order_details: dict) -> None:
    """Store a pending order with timestamp and clean up stale entries."""
    # Evict expired pending orders (>10 min)
    cutoff = _time.time() - _PENDING_ORDER_TTL
    stale = [k for k, v in _pending_orders.items() if v.get("_created_at", 0) < cutoff]
    for k in stale:
        _pending_orders.pop(k, None)
    order_details["_created_at"] = _time.time()
    _pending_orders[order_key] = order_details


# ── Commands ──────────────────────────────────────────────────────────────────


@router.message(Command("start"))
@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Show help with available commands."""
    if not _is_admin(message.from_user.id):
        return
    await message.answer(HELP_TEXT, parse_mode="HTML")


@router.message(Command("vhelp"))
async def cmd_vhelp(message: Message) -> None:
    """Explain what each /v field means."""
    if not _is_admin(message.from_user.id):
        return
    await message.answer(VHELP_TEXT, parse_mode="HTML")


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    """Show full system health."""
    if not _is_admin(message.from_user.id):
        return
    if _on_health:
        try:
            text = await _on_health()
            await message.answer(text, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"Error: {escape(str(e))}")
    else:
        pending_count = len(_pending)
        await message.answer(
            f"\U0001f7e2 IBKR-Telegram is running\n"
            f"Pending confirmations: {pending_count}"
        )


    # /portfolio, /account, /positions, /pnl removed — all covered by /v


@router.message(Command("deposits"))
async def cmd_deposits(message: Message) -> None:
    """Show deposit/withdrawal history."""
    logger.info(f"/deposits from user {message.from_user.id}, admin={_admin_chat_id}")
    if not _is_admin(message.from_user.id):
        return
    if _on_deposits:
        try:
            args = message.text.split(maxsplit=1)
            account_filter = args[1].strip().lower() if len(args) > 1 else "all"
            text = await _on_deposits(account_filter)
            # Split if too long for Telegram (4096 char limit)
            if len(text) > 4000:
                parts = text.split("\n\n")
                chunk = ""
                for part in parts:
                    if len(chunk) + len(part) + 2 > 4000:
                        await message.answer(chunk, parse_mode="HTML")
                        chunk = part
                    else:
                        chunk = chunk + "\n\n" + part if chunk else part
                if chunk:
                    await message.answer(chunk, parse_mode="HTML")
            else:
                await message.answer(text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"/deposits error: {e}", exc_info=True)
            await message.answer(f"Error: {escape(str(e))}")
    else:
        await message.answer("Deposit tracking not connected")


@router.message(Command("signals"))
async def cmd_signals(message: Message) -> None:
    """Show recent signals."""
    if not _is_admin(message.from_user.id):
        return
    if _on_signals:
        try:
            args = message.text.split()
            limit = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
            text = await _on_signals(limit)
            await message.answer(text, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"Error: {escape(str(e))}")
    else:
        await message.answer("Signals not connected")


@router.message(Command("dashboard"))
async def cmd_dashboard(message: Message) -> None:
    """Send the dashboard URL."""
    if not _is_admin(message.from_user.id):
        return
    if _dashboard_url:
        await message.answer(
            f"\U0001f4c8 <b>Dashboard</b>\n\n<a href=\"{_dashboard_url}\">{_dashboard_url}</a>",
            parse_mode="HTML",
            disable_web_page_preview=False,
        )
    else:
        await message.answer("Dashboard URL not configured")


@router.message(Command("pending"))
async def cmd_pending(message: Message) -> None:
    """Show pending trade confirmations."""
    if not _is_admin(message.from_user.id):
        return
    if not _pending:
        await message.answer("No pending confirmations.")
        return
    lines = ["\U0001f552 <b>Pending Confirmations</b>\n"]
    for key, entry in _pending.items():
        signal_id, signal = entry["signal_id"], entry["signal"]
        lines.append(f"  ${escape(signal.ticker)} \u2014 {escape(signal.action)} (id={signal_id})")
    await message.answer("\n".join(lines), parse_mode="HTML")


# ── Quick Trading: /buy (add to existing) ────────────────────────────────────


@router.message(Command("buy"))
async def cmd_buy(message: Message) -> None:
    """Buy/add to existing position. No args = show position picker."""
    if not _is_admin(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)

    # Text shortcut: /buy IREN 5%
    if len(args) > 1 and _on_buy_preview:
        try:
            await message.answer("\U0001f50d Looking up contract...")
            text, order_details = await _on_buy_preview(args[1])
            order_key = f"buy_{order_details.get('ticker', 'x')}_{_next_order_id()}"
            _store_pending_order(order_key, order_details)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="\u2705 Execute", callback_data=f"order:exec:{order_key}"),
                InlineKeyboardButton(text="\u274c Cancel", callback_data=f"order:cancel:{order_key}"),
            ]])
            await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            logger.error(f"/buy error: {e}", exc_info=True)
            await message.answer(f"\u274c {escape(str(e))}")
        return

    # No args: show position picker
    if not _on_list_positions:
        await message.answer("Order system not connected")
        return
    try:
        positions = await _on_list_positions()
        if not positions:
            await message.answer(
                "No open positions. Use <code>/new TICKER</code> to open a new one.",
                parse_mode="HTML",
            )
            return

        # Build grid of position buttons (2 per row)
        rows = []
        row = []
        for p in positions:
            btn_text = f"{p['symbol']} ({p['total_qty']}x)"
            row.append(InlineKeyboardButton(text=btn_text, callback_data=f"a:{p['symbol']}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

        keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
        await message.answer(
            "\U0001f4c8 <b>Add to position</b>\nSelect ticker:",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error(f"/buy list error: {e}", exc_info=True)
        await message.answer(f"\u274c {escape(str(e))}")


# ── Quick Trading: /sell (reduce/close existing) ──────────────────────────────


@router.message(Command("sell"))
async def cmd_sell(message: Message) -> None:
    """Sell/reduce existing position. No args = show position picker."""
    if not _is_admin(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)

    # Text shortcut: /sell IREN all
    if len(args) > 1 and _on_sell_preview:
        try:
            await message.answer("\U0001f50d Looking up position...")
            text, order_details = await _on_sell_preview(args[1])
            order_key = f"sell_{order_details.get('ticker', 'x')}_{_next_order_id()}"
            _store_pending_order(order_key, order_details)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="\u2705 Execute", callback_data=f"order:exec:{order_key}"),
                InlineKeyboardButton(text="\u274c Cancel", callback_data=f"order:cancel:{order_key}"),
            ]])
            await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            logger.error(f"/sell error: {e}", exc_info=True)
            await message.answer(f"\u274c {escape(str(e))}")
        return

    # No args: show position picker
    if not _on_list_positions:
        await message.answer("Order system not connected")
        return
    try:
        positions = await _on_list_positions()
        if not positions:
            await message.answer("No open positions to sell.")
            return

        rows = []
        row = []
        for p in positions:
            btn_text = f"{p['symbol']} ({p['total_qty']}x)"
            row.append(InlineKeyboardButton(text=btn_text, callback_data=f"s:{p['symbol']}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

        keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
        await message.answer(
            "\U0001f4e4 <b>Sell/Close position</b>\nSelect ticker:",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error(f"/sell list error: {e}", exc_info=True)
        await message.answer(f"\u274c {escape(str(e))}")


# ── New Position: /new (full option chain wizard) ─────────────────────────────


@router.message(Command("new"))
async def cmd_new(message: Message) -> None:
    """Open new position with interactive option chain browser."""
    if not _is_admin(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "\U0001f195 <b>New Position</b>\n\n"
            "Type the ticker symbol:",
            parse_mode="HTML",
            reply_markup=ForceReply(selective=True, input_field_placeholder="e.g. IREN"),
        )
        return

    ticker = args[1].strip().upper()
    if not _on_option_expiries:
        await message.answer("Option chain not connected")
        return

    try:
        await message.answer(f"\U0001f50d Loading option chain for <b>{ticker}</b>...", parse_mode="HTML")
        expiries = await _on_option_expiries(ticker)

        # Show expiry buttons (3 per row)
        rows = []
        # Show Call/Put toggle first
        rows.append([
            InlineKeyboardButton(text="\U0001f7e2 Calls", callback_data=f"n:{ticker}:C:exp"),
            InlineKeyboardButton(text="\U0001f534 Puts", callback_data=f"n:{ticker}:P:exp"),
        ])

        # Show expiry buttons (default: calls)
        row = []
        for exp in expiries[:9]:  # Max 9 expiries
            dte_label = f"{exp['display']}({exp['dte']}d)"
            row.append(InlineKeyboardButton(
                text=dte_label,
                callback_data=f"n:{ticker}:{exp['exp']}:C",
            ))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

        keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
        await message.answer(
            f"\U0001f195 <b>{ticker}</b> \u2014 Select expiry\n"
            f"<i>Showing calls. Tap Puts to switch.</i>",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error(f"/new error: {e}", exc_info=True)
        await message.answer(f"\u274c {escape(str(e))}")


@router.message(F.reply_to_message & F.reply_to_message.text.contains("Type the ticker symbol"))
async def handle_new_ticker_reply(message: Message) -> None:
    """Handle reply to /new ForceReply — process as /new TICKER."""
    if not _is_admin(message.from_user.id):
        return
    ticker = message.text.strip().upper()
    if not ticker:
        return

    if not _on_option_expiries:
        await message.answer("Option chain not connected")
        return

    try:
        await message.answer(f"\U0001f50d Loading option chain for <b>{ticker}</b>...", parse_mode="HTML")
        expiries = await _on_option_expiries(ticker)

        rows = []
        rows.append([
            InlineKeyboardButton(text="\U0001f7e2 Calls", callback_data=f"n:{ticker}:C:exp"),
            InlineKeyboardButton(text="\U0001f534 Puts", callback_data=f"n:{ticker}:P:exp"),
        ])
        row = []
        for exp in expiries[:9]:
            dte_label = f"{exp['display']} ({exp['dte']}d)"
            row.append(InlineKeyboardButton(
                text=dte_label,
                callback_data=f"n:{ticker}:{exp['exp']}:C",
            ))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

        keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
        await message.answer(
            f"\U0001f195 <b>{ticker}</b> \u2014 Select expiry\n"
            f"<i>Showing calls. Tap Puts to switch.</i>",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error(f"/new reply error: {e}", exc_info=True)
        await message.answer(f"\u274c {escape(str(e))}")


@router.message(Command("info"))
async def cmd_info(message: Message) -> None:
    """Show full position details: bid/ask, spread, Greeks, P&L."""
    if not _is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        # No ticker: show position picker
        if _on_list_positions:
            positions = await _on_list_positions()
            if not positions:
                await message.answer("No open positions.")
                return
            rows = []
            row = []
            for p in positions:
                btn_text = f"{p['symbol']}"
                row.append(InlineKeyboardButton(text=btn_text, callback_data=f"i:{p['symbol']}"))
                if len(row) == 3:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)
            keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            await message.answer(
                "\U0001f4cb <b>Position Info</b>\nSelect ticker:",
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            await message.answer("Usage: <code>/info TICKER</code>", parse_mode="HTML")
        return

    if not _on_info:
        await message.answer("Info not connected")
        return
    try:
        text = await _on_info(args[1])
        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"\u274c {escape(str(e))}")


@router.message(Command("price"))
async def cmd_price(message: Message) -> None:
    """Quick quote: stock price + option price for held positions."""
    if not _is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        # No ticker: show position picker
        if _on_list_positions:
            positions = await _on_list_positions()
            if not positions:
                await message.answer(
                    "Usage: <code>/price TICKER</code>\nNo open positions.",
                    parse_mode="HTML",
                )
                return
            rows = []
            row = []
            for p in positions:
                row.append(InlineKeyboardButton(text=p['symbol'], callback_data=f"p:{p['symbol']}"))
                if len(row) == 3:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)
            keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            await message.answer(
                "\U0001f4b5 <b>Price Quote</b>\nSelect ticker:",
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            await message.answer("Usage: <code>/price TICKER</code>", parse_mode="HTML")
        return

    if not _on_price:
        await message.answer("Price lookup not connected")
        return
    try:
        text = await _on_price(args[1])
        await message.answer(text, parse_mode="HTML")
    except Exception as e:
        await message.answer(f"\u274c {escape(str(e))}")


@router.callback_query(F.data.startswith("p:"))
async def on_price_pick(callback: CallbackQuery) -> None:
    """User picked a ticker from /price picker."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    ticker = callback.data.removeprefix("p:")
    await callback.answer(f"Loading {ticker}...")
    if _on_price:
        try:
            text = await _on_price(ticker)
            await callback.message.edit_text(text, parse_mode="HTML")
        except Exception as e:
            await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")


@router.message(Command("v"))
async def cmd_value(message: Message) -> None:
    """Show compact portfolio value with positions."""
    if not _is_admin(message.from_user.id):
        return
    if _on_value:
        try:
            text = await _on_value("day")
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="\U0001f4bc Total", callback_data="v:total"),
                InlineKeyboardButton(text="\U0001f504 Refresh", callback_data="v:day"),
                InlineKeyboardButton(text="\u2753", callback_data="v:help"),
            ]])
            await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            logger.error(f"/v error: {e}", exc_info=True)
            await message.answer(f"Error: {escape(str(e))}")
    else:
        await message.answer("Value tracking not connected")


@router.message(Command("pause"))
async def cmd_pause(message: Message) -> None:
    """Pause IB Gateways so the user can log into IBKR."""
    if not _is_admin(message.from_user.id):
        return
    if not _on_pause:
        await message.answer("Pause not available")
        return

    # Parse optional minutes argument (default 10)
    args = (message.text or "").split()
    minutes = 10
    if len(args) > 1:
        try:
            minutes = int(args[1])
            minutes = max(1, min(minutes, 60))  # clamp 1-60
        except ValueError:
            pass

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"\u23f8 Pause {minutes}min",
            callback_data=f"pause:{minutes}",
        ),
        InlineKeyboardButton(text="Cancel", callback_data="pause:cancel"),
    ]])
    await message.answer(
        f"\u23f8 <b>Pause IB Gateways for {minutes} minutes?</b>\n\n"
        "Gateways will stop so you can log into IBKR.\n"
        "They will auto-restart after the timer expires.\n"
        "Trading commands will be unavailable during the pause.",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


@router.callback_query(F.data.startswith("pause:"))
async def on_pause_callback(callback: CallbackQuery) -> None:
    """Handle pause confirmation/cancellation/resume."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    action = callback.data.removeprefix("pause:")

    if action == "cancel":
        await callback.answer("Cancelled")
        await callback.message.edit_text("\u23f8 Pause cancelled.")
        return

    if action == "resume":
        await callback.answer("Resuming gateways...")
        await callback.message.edit_text("\u25b6\ufe0f Resuming gateways...")
        if _on_resume:
            result = await _on_resume()
            await callback.message.edit_text(result, parse_mode="HTML")
        return

    # action is the number of minutes
    try:
        minutes = int(action)
    except ValueError:
        await callback.answer("Invalid")
        return

    await callback.answer(f"Pausing for {minutes} minutes...")
    await callback.message.edit_text(f"\u23f8 Stopping gateways for {minutes} minutes...")

    if _on_pause:
        result = await _on_pause(minutes)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="\u25b6\ufe0f Resume Now", callback_data="pause:resume"),
        ]])
        await callback.message.edit_text(result, parse_mode="HTML", reply_markup=keyboard)


@router.message(Command("orders"))
async def cmd_orders(message: Message) -> None:
    """Show open orders with cancel buttons."""
    if not _is_admin(message.from_user.id):
        return
    if not _on_orders:
        await message.answer("Order tracking not connected")
        return
    try:
        text, orders = await _on_orders()
        if not orders:
            await message.answer(text, parse_mode="HTML")
            return

        # Build cancel buttons (1 per order + cancel all)
        rows = []
        for o in orders:
            btn_text = f"\u274c #{o['order_id']} {o['action']} {o['qty']}x {o['symbol']}"
            cb = f"cx:{o['account_name']}:{o['order_id']}"
            rows.append([InlineKeyboardButton(text=btn_text, callback_data=cb)])

        if len(orders) > 1:
            rows.append([InlineKeyboardButton(
                text="\U0001f534 CANCEL ALL ORDERS",
                callback_data="cx:all",
            )])

        rows.append([InlineKeyboardButton(
            text="\U0001f504 Refresh",
            callback_data="cx:refresh",
        )])

        keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"/orders error: {e}", exc_info=True)
        await message.answer(f"\u274c {escape(str(e))}")


@router.message(Command("trades"))
async def cmd_trades(message: Message) -> None:
    """Show trade execution history."""
    if not _is_admin(message.from_user.id):
        return
    if not _on_trades:
        await message.answer("Trade history not connected")
        return
    try:
        text = await _on_trades("today")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="\U0001f4c5 Today", callback_data="trades:today"),
            InlineKeyboardButton(text="\U0001f4c6 Past Week", callback_data="trades:week"),
        ]])
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"/trades error: {e}", exc_info=True)
        await message.answer(f"\u274c {escape(str(e))}")


@router.callback_query(F.data.startswith("trades:"))
async def on_trades_period(callback: CallbackQuery) -> None:
    """Switch trade history period (today/week)."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    period = callback.data.removeprefix("trades:")
    if not _on_trades:
        await callback.answer("Not connected", show_alert=True)
        return
    await callback.answer()
    try:
        text = await _on_trades(period)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="\U0001f4c5 Today", callback_data="trades:today"),
            InlineKeyboardButton(text="\U0001f4c6 Past Week", callback_data="trades:week"),
        ]])
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")


@router.message(Command("kill"))
async def cmd_kill(message: Message) -> None:
    """Cancel all open orders on all accounts."""
    if not _is_admin(message.from_user.id):
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="\U0001f534 CANCEL ALL ORDERS", callback_data="kill:confirm"),
            InlineKeyboardButton(text="Back", callback_data="kill:cancel"),
        ],
    ])
    await message.answer(
        "\u26a0\ufe0f <b>Cancel All Open Orders</b>\n\n"
        "This will cancel ALL pending/working orders on ALL accounts.\n"
        "Existing positions are NOT affected.\n"
        "Are you sure?",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ── Callbacks: Signal confirmation ────────────────────────────────────────────


_executed_signals: dict[str, float] = {}  # signal_key -> timestamp (dedup)


@router.callback_query(F.data.startswith("exec:"))
async def on_execute(callback: CallbackQuery) -> None:
    """Handle trade execution confirmation."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    signal_key = callback.data.removeprefix("exec:")

    # Dedup: prevent double-execution from rapid taps or Telegram retries
    if signal_key in _executed_signals:
        await callback.answer("Signal already submitted", show_alert=True)
        return

    pending = _pending.pop(signal_key, None)

    if not pending:
        await callback.answer("Signal expired or already processed")
        return

    _executed_signals[signal_key] = _time.time()
    # Cleanup old entries (>5 min)
    cutoff = _time.time() - 300
    for k in [k for k, t in _executed_signals.items() if t < cutoff]:
        del _executed_signals[k]

    signal_id, signal = pending["signal_id"], pending["signal"]
    await callback.answer(f"Executing {signal.action} ${signal.ticker}...")

    if _on_confirm:
        try:
            results = await _on_confirm(signal_id, signal)
            result_lines = []
            for r in results:
                if r.success:
                    result_lines.append(
                        f"\u2705 {escape(r.account_name)}: filled {r.filled_qty} @ ${r.avg_price:.2f}"
                    )
                else:
                    result_lines.append(f"\u274c {escape(r.account_name)}: {escape(str(r.error))}")
            result_text = "\n".join(result_lines) if result_lines else "No accounts connected"

            await callback.message.edit_text(
                callback.message.text + f"\n\n<b>EXECUTED</b>\n{result_text}",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Execution failed: {e}", exc_info=True)
            await callback.message.edit_text(
                callback.message.text + f"\n\n\u274c <b>EXECUTION FAILED</b>\n{escape(str(e))}",
                parse_mode="HTML",
            )
    else:
        await callback.message.edit_text(
            callback.message.text + "\n\n\u2705 <b>CONFIRMED</b> (executor not connected)",
            parse_mode="HTML",
        )

    logger.info(f"Trade {signal_key} confirmed for execution")


@router.callback_query(F.data.startswith("skip:"))
async def on_skip(callback: CallbackQuery) -> None:
    """Handle trade skip."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    signal_key = callback.data.removeprefix("skip:")
    _pending.pop(signal_key, None)
    await callback.answer("Trade skipped")
    await callback.message.edit_text(
        callback.message.text + "\n\n\u274c <b>SKIPPED</b>",
        parse_mode="HTML",
    )
    logger.info(f"Trade {signal_key} skipped")


@router.callback_query(F.data == "kill:confirm")
async def on_kill_confirm(callback: CallbackQuery) -> None:
    """Cancel all open orders."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    if _on_kill:
        try:
            await callback.answer("Cancelling all orders...")
            result = await _on_kill()
            await callback.message.edit_text(
                f"\u26a0\ufe0f <b>Orders Cancelled</b>\n\n{result}",
                parse_mode="HTML",
            )
        except Exception as e:
            await callback.message.edit_text(
                f"\u274c Cancel orders error: {escape(str(e))}",
                parse_mode="HTML",
            )
    else:
        await callback.message.edit_text("Order cancellation not connected to executor")


@router.callback_query(F.data == "kill:cancel")
async def on_kill_cancel(callback: CallbackQuery) -> None:
    """Cancel kill switch."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    await callback.message.edit_text("\u2705 Kill switch cancelled.")
    await callback.answer()


# ── Callbacks: /orders ───────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("cx:"))
async def on_orders_action(callback: CallbackQuery) -> None:
    """Handle /orders cancel buttons."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    action = callback.data.removeprefix("cx:")

    if action == "refresh":
        if not _on_orders:
            await callback.answer("Not connected")
            return
        await callback.answer("Refreshing...")
        try:
            text, orders = await _on_orders()
            if not orders:
                await callback.message.edit_text(text, parse_mode="HTML")
                return
            rows = []
            for o in orders:
                btn_text = f"\u274c #{o['order_id']} {o['action']} {o['qty']}x {o['symbol']}"
                cb = f"cx:{o['account_name']}:{o['order_id']}"
                rows.append([InlineKeyboardButton(text=btn_text, callback_data=cb)])
            if len(orders) > 1:
                rows.append([InlineKeyboardButton(
                    text="\U0001f534 CANCEL ALL ORDERS",
                    callback_data="cx:all",
                )])
            rows.append([InlineKeyboardButton(
                text="\U0001f504 Refresh",
                callback_data="cx:refresh",
            )])
            keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            await callback.answer(f"Error: {e}", show_alert=True)
        return

    if action == "all":
        if not _on_cancel_all:
            await callback.answer("Not connected")
            return
        await callback.answer("Cancelling all orders...")
        try:
            result = await _on_cancel_all()
            await callback.message.edit_text(
                f"\U0001f534 <b>All Orders Cancelled</b>\n\n{result}",
                parse_mode="HTML",
            )
        except Exception as e:
            await callback.answer(f"Error: {e}", show_alert=True)
        return

    # action = "account_name:order_id"
    parts = action.rsplit(":", 1)
    if len(parts) != 2:
        await callback.answer("Invalid action")
        return

    account_name, order_id_str = parts
    try:
        order_id = int(order_id_str)
    except ValueError:
        await callback.answer("Invalid order ID")
        return

    if not _on_cancel_order:
        await callback.answer("Not connected")
        return

    await callback.answer(f"Cancelling #{order_id}...")
    try:
        result = await _on_cancel_order(account_name, order_id)
        await callback.message.edit_text(
            callback.message.text + f"\n\n{result}",
            parse_mode="HTML",
        )
    except Exception as e:
        await callback.answer(f"Error: {e}", show_alert=True)


# ── Callbacks: /v toggle ──────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("v:"))
async def on_value_toggle(callback: CallbackQuery) -> None:
    """Handle /v inline button toggles (day/total/refresh)."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    mode = callback.data.removeprefix("v:")
    if mode == "help":
        await callback.answer()
        await callback.message.answer(VHELP_TEXT, parse_mode="HTML")
        return
    if _on_value:
        try:
            await callback.answer("Refreshing...")
            text = await _on_value(mode)
            if mode == "day":
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="\U0001f4bc Total", callback_data="v:total"),
                    InlineKeyboardButton(text="\U0001f504 Refresh", callback_data="v:day"),
                    InlineKeyboardButton(text="\u2753", callback_data="v:help"),
                ]])
            else:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="\U0001f4c8 Day", callback_data="v:day"),
                    InlineKeyboardButton(text="\U0001f504 Refresh", callback_data="v:total"),
                    InlineKeyboardButton(text="\u2753", callback_data="v:help"),
                ]])
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            await callback.answer(f"Error: {e}", show_alert=True)
    else:
        await callback.answer("Not connected")


# ── Callbacks: /info ──────────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("i:"))
async def on_info_pick(callback: CallbackQuery) -> None:
    """User picked a ticker from /info picker."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    ticker = callback.data.removeprefix("i:")
    await callback.answer(f"Loading {ticker}...")
    if _on_info:
        try:
            text = await _on_info(ticker)
            await callback.message.edit_text(text, parse_mode="HTML")
        except Exception as e:
            await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")


# ── Callbacks: /buy quick flow ────────────────────────────────────────────────


@router.callback_query(F.data.startswith("a:"))
async def on_buy_pick(callback: CallbackQuery) -> None:
    """User picked a ticker to add to — show percentage buttons."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    # Parse: "a:IREN" or "a:IREN:5" (ticker or ticker:pct)
    parts = callback.data.split(":")
    ticker = parts[1]

    if len(parts) == 3:
        # User picked a percentage — go to preview
        pct = parts[2]
        await callback.answer("Building preview...")
        if _on_buy_preview:
            try:
                text, order_details = await _on_buy_preview(f"{ticker} {pct} mkt")
                order_key = f"buy_{ticker}_{_next_order_id()}"
                _store_pending_order(order_key, order_details)
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="\u2705 Execute", callback_data=f"order:exec:{order_key}"),
                    InlineKeyboardButton(text="\u274c Cancel", callback_data=f"order:cancel:{order_key}"),
                ]])
                await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
            except Exception as e:
                await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")
        return

    # Show percentage picker
    await callback.answer()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="3%", callback_data=f"a:{ticker}:3"),
            InlineKeyboardButton(text="5%", callback_data=f"a:{ticker}:5"),
            InlineKeyboardButton(text="8%", callback_data=f"a:{ticker}:8"),
            InlineKeyboardButton(text="10%", callback_data=f"a:{ticker}:10"),
        ],
        [
            InlineKeyboardButton(text="12%", callback_data=f"a:{ticker}:12"),
            InlineKeyboardButton(text="15%", callback_data=f"a:{ticker}:15"),
            InlineKeyboardButton(text="20%", callback_data=f"a:{ticker}:20"),
        ],
    ])
    await callback.message.edit_text(
        f"\U0001f4c8 <b>Add to {ticker}</b>\n"
        f"Select % of NLV to allocate:",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ── Callbacks: /sell quick flow ───────────────────────────────────────────────


@router.callback_query(F.data.startswith("s:"))
async def on_sell_pick(callback: CallbackQuery) -> None:
    """User picked a ticker to sell — show fraction buttons."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    parts = callback.data.split(":")
    ticker = parts[1]

    if len(parts) == 3:
        # User picked a fraction — go to preview
        fraction = parts[2]
        await callback.answer("Building preview...")
        if _on_sell_preview:
            try:
                text, order_details = await _on_sell_preview(f"{ticker} {fraction} mkt")
                order_key = f"sell_{ticker}_{_next_order_id()}"
                _store_pending_order(order_key, order_details)
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="\u2705 Execute", callback_data=f"order:exec:{order_key}"),
                    InlineKeyboardButton(text="\u274c Cancel", callback_data=f"order:cancel:{order_key}"),
                ]])
                await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
            except Exception as e:
                await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")
        return

    # Show fraction picker
    await callback.answer()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="All (100%)", callback_data=f"s:{ticker}:all"),
            InlineKeyboardButton(text="Half (50%)", callback_data=f"s:{ticker}:half"),
        ],
        [
            InlineKeyboardButton(text="Third (33%)", callback_data=f"s:{ticker}:third"),
            InlineKeyboardButton(text="Quarter (25%)", callback_data=f"s:{ticker}:quarter"),
        ],
    ])
    await callback.message.edit_text(
        f"\U0001f4e4 <b>Sell {ticker}</b>\n"
        f"How much to sell?",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ── Callbacks: /new option chain wizard ───────────────────────────────────────


@router.callback_query(F.data.startswith("n:"))
async def on_new_chain(callback: CallbackQuery) -> None:
    """Multi-step option chain wizard.

    Data formats:
      n:TICKER:C:exp       — switch to calls, show expiries
      n:TICKER:P:exp       — switch to puts, show expiries
      n:TICKER:EXPIRY:R    — show strikes for expiry (R=C or P)
      n:TICKER:EXPIRY:R:S  — show % picker (S=strike)
      n:TICKER:EXPIRY:R:S:P — preview (P=pct)
    """
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return

    parts = callback.data.split(":")
    ticker = parts[1]

    # n:TICKER:C:exp or n:TICKER:P:exp — switch right, show expiries
    if len(parts) == 4 and parts[3] == "exp":
        right = parts[2]  # C or P
        await callback.answer(f"Loading {'calls' if right == 'C' else 'puts'}...")
        if not _on_option_expiries:
            await callback.answer("Not connected", show_alert=True)
            return
        try:
            expiries = await _on_option_expiries(ticker)
            rows = [[
                InlineKeyboardButton(text="\U0001f7e2 Calls", callback_data=f"n:{ticker}:C:exp"),
                InlineKeyboardButton(text="\U0001f534 Puts", callback_data=f"n:{ticker}:P:exp"),
            ]]
            row = []
            for exp in expiries[:9]:
                dte_label = f"{exp['display']} ({exp['dte']}d)"
                row.append(InlineKeyboardButton(
                    text=dte_label,
                    callback_data=f"n:{ticker}:{exp['exp']}:{right}",
                ))
                if len(row) == 3:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)
            keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            right_label = "calls" if right == "C" else "puts"
            await callback.message.edit_text(
                f"\U0001f195 <b>{ticker}</b> \u2014 Select expiry\n"
                f"<i>Showing {right_label}.</i>",
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        except Exception as e:
            await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")
        return

    # n:TICKER:EXPIRY:RIGHT — show strikes
    if len(parts) == 4 and len(parts[2]) == 8 and parts[2].isdigit():
        expiry = parts[2]
        right = parts[3]
        await callback.answer("Loading strikes...")
        if not _on_option_strikes:
            await callback.answer("Not connected", show_alert=True)
            return
        try:
            data = await _on_option_strikes(ticker, expiry, right)
            strikes = data["strikes"]
            current_price = data["current_price"]
            exp_display = f"{expiry[4:6]}/{expiry[0:4]}"

            rows = []
            row = []
            for s in strikes:
                strike_val = s["strike"]
                label = s["label"]
                # Format: "$27 ITM" or "$85 OTM"
                strike_str = f"${strike_val:g}"
                if strike_val == int(strike_val):
                    strike_str = f"${int(strike_val)}"
                btn_text = f"{strike_str} {label}"
                # Callback: n:TICKER:EXPIRY:RIGHT:STRIKE (keep short for 64-byte limit)
                strike_cb = f"{strike_val:.2f}".rstrip("0").rstrip(".")
                cb = f"n:{ticker}:{expiry}:{right}:{strike_cb}"
                row.append(InlineKeyboardButton(text=btn_text, callback_data=cb))
                if len(row) == 3:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)

            # Back button
            rows.append([InlineKeyboardButton(
                text="\u2b05 Back to expiries",
                callback_data=f"n:{ticker}:{right}:exp",
            )])

            keyboard = InlineKeyboardMarkup(inline_keyboard=rows)
            right_label = "Call" if right == "C" else "Put"
            await callback.message.edit_text(
                f"\U0001f195 <b>{ticker} {right_label}</b> exp {exp_display}\n"
                f"Current: ${current_price:.2f}\n\n"
                f"Select strike:",
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        except Exception as e:
            await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")
        return

    # n:TICKER:EXPIRY:RIGHT:STRIKE — show % picker
    if len(parts) == 5:
        expiry = parts[2]
        right = parts[3]
        strike = parts[4]
        await callback.answer()

        exp_display = f"{expiry[4:6]}/{expiry[0:4]}"
        right_label = "Call" if right == "C" else "Put"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="3%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:3"),
                InlineKeyboardButton(text="5%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:5"),
                InlineKeyboardButton(text="8%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:8"),
                InlineKeyboardButton(text="10%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:10"),
            ],
            [
                InlineKeyboardButton(text="12%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:12"),
                InlineKeyboardButton(text="15%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:15"),
                InlineKeyboardButton(text="20%", callback_data=f"n:{ticker}:{expiry}:{right}:{strike}:20"),
            ],
            [InlineKeyboardButton(
                text="\u2b05 Back to strikes",
                callback_data=f"n:{ticker}:{expiry}:{right}",
            )],
        ])
        await callback.message.edit_text(
            f"\U0001f195 <b>{ticker}</b> ${strike} {right_label} exp {exp_display}\n\n"
            f"Select % of NLV to allocate:",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return

    # n:TICKER:EXPIRY:RIGHT:STRIKE:PCT — final preview
    if len(parts) == 6:
        expiry = parts[2]
        right = parts[3]
        strike = parts[4]
        pct = parts[5]
        await callback.answer("Building preview...")

        if not _on_buy_preview:
            await callback.answer("Not connected", show_alert=True)
            return

        try:
            # Build the full args string for _on_buy_preview
            # Format: "TICKER PCT% MMMYY STRIKEr"
            month_num = expiry[4:6]
            year_short = expiry[2:4]
            months = {
                "01": "JAN", "02": "FEB", "03": "MAR", "04": "APR",
                "05": "MAY", "06": "JUN", "07": "JUL", "08": "AUG",
                "09": "SEP", "10": "OCT", "11": "NOV", "12": "DEC",
            }
            exp_arg = f"{months[month_num]}{year_short}"
            raw_args = f"{ticker} {pct} {exp_arg} {strike}{right} mkt"

            text, order_details = await _on_buy_preview(raw_args)
            order_key = f"new_{ticker}_{_next_order_id()}"
            _store_pending_order(order_key, order_details)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="\u2705 Execute", callback_data=f"order:exec:{order_key}"),
                InlineKeyboardButton(text="\u274c Cancel", callback_data=f"order:cancel:{order_key}"),
            ]])
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            await callback.message.edit_text(f"\u274c {escape(str(e))}", parse_mode="HTML")
        return

    await callback.answer("Unknown action")


# ── Callbacks: Order execution/cancel ─────────────────────────────────────────


@router.callback_query(F.data.startswith("order:exec:"))
async def on_order_execute(callback: CallbackQuery) -> None:
    """Execute a pending buy/sell order (with dedup protection)."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    order_key = callback.data.removeprefix("order:exec:")

    # Race condition guard: prevent double-click execution
    if order_key in _executed_orders:
        await callback.answer("Order already submitted")
        return

    order_details = _pending_orders.pop(order_key, None)
    if not order_details:
        await callback.answer("Order expired or already processed")
        return

    # Mark as executed BEFORE any async work
    _executed_orders[order_key] = _time.time()
    # Cleanup old entries (>5 min)
    cutoff = _time.time() - 300
    for k in [k for k, t in _executed_orders.items() if t < cutoff]:
        _executed_orders.pop(k, None)

    await callback.answer("Executing order...")
    action = order_details.get("action", "BUY")

    try:
        if action == "BUY" and _on_buy_execute:
            result = await _on_buy_execute(order_details)
        elif action == "SELL" and _on_sell_execute:
            result = await _on_sell_execute(order_details)
        else:
            result = "Executor not connected"

        await callback.message.edit_text(
            callback.message.text + f"\n\n\u2705 <b>ORDERS PLACED</b>\n{result}",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Order execution failed: {e}", exc_info=True)
        await callback.message.edit_text(
            callback.message.text + f"\n\n\u274c <b>FAILED</b>\n{escape(str(e))}",
            parse_mode="HTML",
        )


@router.callback_query(F.data.startswith("order:cancel:"))
async def on_order_cancel(callback: CallbackQuery) -> None:
    """Cancel a pending order."""
    if not _is_admin(callback.from_user.id):
        await callback.answer("Unauthorized", show_alert=True)
        return
    order_key = callback.data.removeprefix("order:cancel:")
    _pending_orders.pop(order_key, None)
    await callback.message.edit_text(
        callback.message.text + "\n\n\u274c <b>CANCELLED</b>",
        parse_mode="HTML",
    )
    await callback.answer("Order cancelled")
