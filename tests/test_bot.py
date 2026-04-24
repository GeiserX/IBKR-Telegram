"""Tests for the Telegram bot — handlers, confirmation flow, helpers."""

import time as _time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.bot import (
    HELP_TEXT,
    VHELP_TEXT,
    ConfirmationBot,
    _is_admin,
    _next_order_id,
    _store_pending_order,
    cmd_buy,
    cmd_dashboard,
    cmd_deposits,
    cmd_help,
    cmd_info,
    cmd_kill,
    cmd_new,
    cmd_orders,
    cmd_pause,
    cmd_pending,
    cmd_price,
    cmd_sell,
    cmd_signals,
    cmd_status,
    cmd_trades,
    cmd_value,
    cmd_vhelp,
    handle_new_ticker_reply,
    on_buy_pick,
    on_execute,
    on_info_pick,
    on_kill_cancel,
    on_kill_confirm,
    on_new_chain,
    on_order_cancel,
    on_order_execute,
    on_orders_action,
    on_pause_callback,
    on_price_pick,
    on_sell_pick,
    on_skip,
    on_trades_period,
    on_value_toggle,
)
from src.models import TradeSignal

FAKE_TOKEN = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
ADMIN_ID = 99999


# ── Fixtures ────────────────────────────────────────────────────────────────────


_CALLBACK_NAMES = [
    "_on_confirm", "_on_positions", "_on_kill", "_on_portfolio", "_on_account",
    "_on_signals", "_on_health", "_on_deposits", "_on_value", "_on_buy_preview",
    "_on_buy_execute", "_on_sell_preview", "_on_sell_execute", "_on_list_positions",
    "_on_option_expiries", "_on_option_strikes", "_on_info", "_on_pause", "_on_resume",
    "_on_orders", "_on_cancel_order", "_on_cancel_all", "_on_price", "_on_trades",
]


def _reset_bot_module():
    """Reset all module-level globals to pristine state."""
    import src.bot as bot_mod

    bot_mod._pending = {}
    bot_mod._pending_orders = {}
    bot_mod._executed_orders = {}
    bot_mod._executed_signals = {}
    bot_mod._order_counter = 0
    bot_mod._admin_chat_id = 0
    bot_mod._bot_instance = None
    bot_mod._dashboard_url = ""
    for name in _CALLBACK_NAMES:
        setattr(bot_mod, name, None)
    # Detach the module-level router from any Dispatcher so it can be reattached
    bot_mod.router._parent_router = None


@pytest.fixture(autouse=True)
def _reset_globals():
    """Reset all module-level globals before and after each test."""
    _reset_bot_module()
    yield
    _reset_bot_module()


@pytest.fixture()
def bot_instance():
    """Create a ConfirmationBot that sets all globals, with mocked Bot."""
    bot = ConfirmationBot(bot_token=FAKE_TOKEN, admin_chat_id=ADMIN_ID)
    bot.bot = MagicMock()
    bot.bot.send_message = AsyncMock()
    bot.bot.set_my_commands = AsyncMock()
    bot.bot.session = MagicMock()
    bot.bot.session.close = AsyncMock()
    return bot


def _make_message(user_id: int = ADMIN_ID, text: str = "/help") -> MagicMock:
    """Build a mock Message with standard attributes."""
    msg = MagicMock()
    msg.from_user = MagicMock()
    msg.from_user.id = user_id
    msg.text = text
    msg.answer = AsyncMock()
    return msg


def _make_callback(
    user_id: int = ADMIN_ID,
    data: str = "",
    message_text: str = "original text",
) -> MagicMock:
    """Build a mock CallbackQuery."""
    cb = MagicMock()
    cb.from_user = MagicMock()
    cb.from_user.id = user_id
    cb.data = data
    cb.answer = AsyncMock()
    cb.message = MagicMock()
    cb.message.text = message_text
    cb.message.edit_text = AsyncMock()
    cb.message.answer = AsyncMock()
    return cb


# ── ConfirmationBot class ───────────────────────────────────────────────────────


class TestConfirmationBotInit:
    def test_sets_admin_chat_id(self, bot_instance):
        import src.bot as bot_mod

        assert bot_mod._admin_chat_id == ADMIN_ID

    def test_sets_callbacks(self):
        import src.bot as bot_mod

        on_kill = AsyncMock()
        on_confirm = AsyncMock()
        ConfirmationBot(
            bot_token=FAKE_TOKEN,
            admin_chat_id=ADMIN_ID,
            on_kill=on_kill,
            on_confirm=on_confirm,
        )
        assert bot_mod._on_kill is on_kill
        assert bot_mod._on_confirm is on_confirm

    def test_sets_dashboard_url(self):
        import src.bot as bot_mod

        ConfirmationBot(
            bot_token=FAKE_TOKEN,
            admin_chat_id=ADMIN_ID,
            dashboard_url="https://example.com",
        )
        assert bot_mod._dashboard_url == "https://example.com"


class TestGetPendingCount:
    def test_empty(self, bot_instance):
        assert bot_instance.get_pending_count() == 0

    def test_with_pending(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 1, "signal": signal, "_created_at": 0}
        bot_mod._pending["key2"] = {"signal_id": 2, "signal": signal, "_created_at": 0}
        assert bot_instance.get_pending_count() == 2


class TestSendConfirmation:
    async def test_sends_message_with_keyboard(self, bot_instance):
        signal = TradeSignal(
            ticker="AAPL",
            action="BUY",
            target_weight_pct=10.0,
            amount_description="10% of NLV",
            related_ticker="MSFT",
            source="manual",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        )
        await bot_instance.send_confirmation(signal, signal_id=42)

        bot_instance.bot.send_message.assert_awaited_once()
        call_kwargs = bot_instance.bot.send_message.call_args
        assert call_kwargs[0][0] == ADMIN_ID  # chat_id
        text = call_kwargs[0][1]
        assert "AAPL" in text
        assert "BUY" in text
        assert "10%" in text
        assert "10% of NLV" in text
        assert "MSFT" in text
        assert "manual" in text
        assert call_kwargs[1]["parse_mode"] == "HTML"
        # Keyboard has exec and skip buttons
        keyboard = call_kwargs[1]["reply_markup"]
        buttons = keyboard.inline_keyboard[0]
        assert len(buttons) == 2
        assert buttons[0].callback_data.startswith("exec:")
        assert buttons[1].callback_data.startswith("skip:")

    async def test_stores_pending(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(
            ticker="TSLA",
            action="SELL",
            timestamp=datetime(2024, 6, 1, tzinfo=UTC),
        )
        await bot_instance.send_confirmation(signal, signal_id=7)
        assert len(bot_mod._pending) == 1
        key = list(bot_mod._pending.keys())[0]
        assert "TSLA" in key
        assert "SELL" in key
        stored = bot_mod._pending[key]
        assert stored["signal_id"] == 7
        assert stored["signal"] is signal
        assert "_created_at" in stored

    async def test_no_optional_fields(self, bot_instance):
        """Signal without optional fields still sends."""
        signal = TradeSignal(ticker="GME", action="BUY", source="test")
        await bot_instance.send_confirmation(signal, signal_id=0)
        bot_instance.bot.send_message.assert_awaited_once()
        text = bot_instance.bot.send_message.call_args[0][1]
        assert "Target Weight" not in text
        assert "Amount" not in text
        assert "Related" not in text


class TestSendNotification:
    async def test_sends_plain_html(self, bot_instance):
        await bot_instance.send_notification("<b>Hello</b>")
        bot_instance.bot.send_message.assert_awaited_once_with(
            ADMIN_ID, "<b>Hello</b>", parse_mode="HTML",
        )


class TestStartStop:
    async def test_stop_closes_session(self, bot_instance):
        await bot_instance.stop()
        bot_instance.bot.session.close.assert_awaited_once()


# ── Helper functions ─────────────────────────────────────────────────────────────


class TestIsAdmin:
    def test_admin_returns_true(self, bot_instance):
        assert _is_admin(ADMIN_ID) is True

    def test_non_admin_returns_false(self, bot_instance):
        assert _is_admin(12345) is False

    def test_zero_before_init(self):
        import src.bot as bot_mod

        bot_mod._admin_chat_id = 0
        assert _is_admin(0) is False  # 0 means unconfigured
        assert _is_admin(1) is False


class TestNextOrderId:
    def test_monotonic(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._order_counter = 0
        a = _next_order_id()
        b = _next_order_id()
        c = _next_order_id()
        assert a == 1
        assert b == 2
        assert c == 3
        assert a < b < c


class TestStorePendingOrder:
    def test_stores_with_timestamp(self, bot_instance):
        import src.bot as bot_mod

        _store_pending_order("key1", {"ticker": "AAPL"})
        assert "key1" in bot_mod._pending_orders
        assert "_created_at" in bot_mod._pending_orders["key1"]
        assert bot_mod._pending_orders["key1"]["ticker"] == "AAPL"

    def test_evicts_stale_entries(self, bot_instance):
        import src.bot as bot_mod

        # Insert a stale entry
        bot_mod._pending_orders["old"] = {"_created_at": _time.time() - 700}
        _store_pending_order("new", {"ticker": "TSLA"})
        assert "old" not in bot_mod._pending_orders
        assert "new" in bot_mod._pending_orders

    def test_keeps_fresh_entries(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._pending_orders["fresh"] = {"_created_at": _time.time() - 100}
        _store_pending_order("new", {"ticker": "GME"})
        assert "fresh" in bot_mod._pending_orders
        assert "new" in bot_mod._pending_orders


# ── Command handlers: admin check ────────────────────────────────────────────


class TestAdminGuard:
    """Non-admin users should be silently rejected on all commands."""

    async def test_help_non_admin(self, bot_instance):
        msg = _make_message(user_id=1)
        await cmd_help(msg)
        msg.answer.assert_not_awaited()

    async def test_vhelp_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/vhelp")
        await cmd_vhelp(msg)
        msg.answer.assert_not_awaited()

    async def test_status_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/status")
        await cmd_status(msg)
        msg.answer.assert_not_awaited()

    async def test_deposits_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/deposits")
        await cmd_deposits(msg)
        msg.answer.assert_not_awaited()

    async def test_signals_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/signals")
        await cmd_signals(msg)
        msg.answer.assert_not_awaited()

    async def test_pending_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/pending")
        await cmd_pending(msg)
        msg.answer.assert_not_awaited()

    async def test_buy_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/buy AAPL 5%")
        await cmd_buy(msg)
        msg.answer.assert_not_awaited()

    async def test_sell_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/sell AAPL all")
        await cmd_sell(msg)
        msg.answer.assert_not_awaited()

    async def test_new_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/new IREN")
        await cmd_new(msg)
        msg.answer.assert_not_awaited()

    async def test_info_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/info AAPL")
        await cmd_info(msg)
        msg.answer.assert_not_awaited()

    async def test_price_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/price AAPL")
        await cmd_price(msg)
        msg.answer.assert_not_awaited()

    async def test_value_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/v")
        await cmd_value(msg)
        msg.answer.assert_not_awaited()

    async def test_kill_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/kill")
        await cmd_kill(msg)
        msg.answer.assert_not_awaited()

    async def test_orders_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/orders")
        await cmd_orders(msg)
        msg.answer.assert_not_awaited()

    async def test_trades_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/trades")
        await cmd_trades(msg)
        msg.answer.assert_not_awaited()

    async def test_pause_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/pause")
        await cmd_pause(msg)
        msg.answer.assert_not_awaited()

    async def test_dashboard_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="/dashboard")
        await cmd_dashboard(msg)
        msg.answer.assert_not_awaited()


class TestCallbackAdminGuard:
    """Non-admin users should get Unauthorized on callback queries."""

    async def test_exec_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="exec:somekey")
        await on_execute(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_skip_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="skip:somekey")
        await on_skip(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_kill_confirm_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="kill:confirm")
        await on_kill_confirm(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_kill_cancel_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="kill:cancel")
        await on_kill_cancel(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_value_toggle_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="v:day")
        await on_value_toggle(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_order_execute_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="order:exec:key")
        await on_order_execute(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_order_cancel_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="order:cancel:key")
        await on_order_cancel(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_pause_callback_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="pause:10")
        await on_pause_callback(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_orders_action_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="cx:refresh")
        await on_orders_action(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_new_chain_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="n:IREN:C:exp")
        await on_new_chain(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_buy_pick_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="a:IREN")
        await on_buy_pick(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_sell_pick_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="s:IREN")
        await on_sell_pick(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_info_pick_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="i:AAPL")
        await on_info_pick(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_price_pick_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="p:AAPL")
        await on_price_pick(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)

    async def test_trades_period_non_admin(self, bot_instance):
        cb = _make_callback(user_id=1, data="trades:today")
        await on_trades_period(cb)
        cb.answer.assert_awaited_once_with("Unauthorized", show_alert=True)


# ── Command handlers: happy paths ────────────────────────────────────────────


class TestCmdHelp:
    async def test_returns_help_text(self, bot_instance):
        msg = _make_message(text="/help")
        await cmd_help(msg)
        msg.answer.assert_awaited_once()
        assert msg.answer.call_args[0][0] == HELP_TEXT


class TestCmdVhelp:
    async def test_returns_vhelp_text(self, bot_instance):
        msg = _make_message(text="/vhelp")
        await cmd_vhelp(msg)
        msg.answer.assert_awaited_once_with(VHELP_TEXT, parse_mode="HTML")


class TestCmdStatus:
    async def test_with_health_callback(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_health = AsyncMock(return_value="<b>Healthy</b>")
        msg = _make_message(text="/status")
        await cmd_status(msg)
        msg.answer.assert_awaited_once_with("<b>Healthy</b>", parse_mode="HTML")

    async def test_health_callback_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_health = AsyncMock(side_effect=RuntimeError("conn failed"))
        msg = _make_message(text="/status")
        await cmd_status(msg)
        text = msg.answer.call_args[0][0]
        assert "conn failed" in text

    async def test_without_health_callback(self, bot_instance):
        msg = _make_message(text="/status")
        await cmd_status(msg)
        text = msg.answer.call_args[0][0]
        assert "running" in text
        assert "Pending confirmations: 0" in text


class TestCmdDashboard:
    async def test_with_url(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._dashboard_url = "https://dash.example.com"
        msg = _make_message(text="/dashboard")
        await cmd_dashboard(msg)
        text = msg.answer.call_args[0][0]
        assert "https://dash.example.com" in text

    async def test_without_url(self, bot_instance):
        msg = _make_message(text="/dashboard")
        await cmd_dashboard(msg)
        text = msg.answer.call_args[0][0]
        assert "not configured" in text


class TestCmdDeposits:
    async def test_with_callback(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_deposits = AsyncMock(return_value="<b>Deposits</b>")
        msg = _make_message(text="/deposits")
        await cmd_deposits(msg)
        bot_mod._on_deposits.assert_awaited_once_with("all")
        msg.answer.assert_awaited_once_with("<b>Deposits</b>", parse_mode="HTML")

    async def test_with_account_filter(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_deposits = AsyncMock(return_value="filtered")
        msg = _make_message(text="/deposits live")
        await cmd_deposits(msg)
        bot_mod._on_deposits.assert_awaited_once_with("live")

    async def test_long_message_splits(self, bot_instance):
        import src.bot as bot_mod

        long_text = ("A" * 2000 + "\n\n" + "B" * 2000 + "\n\n" + "C" * 500)
        bot_mod._on_deposits = AsyncMock(return_value=long_text)
        msg = _make_message(text="/deposits")
        await cmd_deposits(msg)
        assert msg.answer.await_count >= 2

    async def test_deposits_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_deposits = AsyncMock(side_effect=RuntimeError("db error"))
        msg = _make_message(text="/deposits")
        await cmd_deposits(msg)
        text = msg.answer.call_args[0][0]
        assert "db error" in text

    async def test_without_callback(self, bot_instance):
        msg = _make_message(text="/deposits")
        await cmd_deposits(msg)
        assert "not connected" in msg.answer.call_args[0][0]


class TestCmdSignals:
    async def test_default_limit(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_signals = AsyncMock(return_value="signals list")
        msg = _make_message(text="/signals")
        await cmd_signals(msg)
        bot_mod._on_signals.assert_awaited_once_with(10)

    async def test_custom_limit(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_signals = AsyncMock(return_value="signals list")
        msg = _make_message(text="/signals 5")
        await cmd_signals(msg)
        bot_mod._on_signals.assert_awaited_once_with(5)

    async def test_signals_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_signals = AsyncMock(side_effect=ValueError("bad"))
        msg = _make_message(text="/signals")
        await cmd_signals(msg)
        assert "bad" in msg.answer.call_args[0][0]

    async def test_without_callback(self, bot_instance):
        msg = _make_message(text="/signals")
        await cmd_signals(msg)
        assert "not connected" in msg.answer.call_args[0][0]


class TestCmdPending:
    async def test_no_pending(self, bot_instance):
        msg = _make_message(text="/pending")
        await cmd_pending(msg)
        assert "No pending" in msg.answer.call_args[0][0]

    async def test_with_pending(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 1, "signal": signal, "_created_at": 0}
        msg = _make_message(text="/pending")
        await cmd_pending(msg)
        text = msg.answer.call_args[0][0]
        assert "AAPL" in text
        assert "BUY" in text


class TestCmdValue:
    async def test_with_callback(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_value = AsyncMock(return_value="<b>Portfolio</b>")
        msg = _make_message(text="/v")
        await cmd_value(msg)
        bot_mod._on_value.assert_awaited_once_with("day")
        msg.answer.assert_awaited_once()
        kwargs = msg.answer.call_args[1]
        assert kwargs["parse_mode"] == "HTML"
        # Check keyboard has Total, Refresh, Help buttons
        keyboard = kwargs["reply_markup"]
        buttons = keyboard.inline_keyboard[0]
        assert len(buttons) == 3

    async def test_value_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_value = AsyncMock(side_effect=RuntimeError("timeout"))
        msg = _make_message(text="/v")
        await cmd_value(msg)
        assert "timeout" in msg.answer.call_args[0][0]

    async def test_without_callback(self, bot_instance):
        msg = _make_message(text="/v")
        await cmd_value(msg)
        assert "not connected" in msg.answer.call_args[0][0]


class TestCmdKill:
    async def test_shows_confirmation(self, bot_instance):
        msg = _make_message(text="/kill")
        await cmd_kill(msg)
        msg.answer.assert_awaited_once()
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        buttons = keyboard.inline_keyboard[0]
        assert any("kill:confirm" in b.callback_data for b in buttons)
        assert any("kill:cancel" in b.callback_data for b in buttons)


class TestCmdPause:
    async def test_default_minutes(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_pause = AsyncMock(return_value="paused")
        msg = _make_message(text="/pause")
        await cmd_pause(msg)
        text = msg.answer.call_args[0][0]
        assert "10 minutes" in text

    async def test_custom_minutes(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_pause = AsyncMock(return_value="paused")
        msg = _make_message(text="/pause 30")
        await cmd_pause(msg)
        text = msg.answer.call_args[0][0]
        assert "30 minutes" in text

    async def test_clamps_minutes(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_pause = AsyncMock(return_value="paused")
        msg = _make_message(text="/pause 999")
        await cmd_pause(msg)
        text = msg.answer.call_args[0][0]
        assert "60 minutes" in text

    async def test_no_callback(self, bot_instance):
        msg = _make_message(text="/pause")
        await cmd_pause(msg)
        assert "not available" in msg.answer.call_args[0][0]


class TestCmdBuy:
    async def test_with_args_and_preview(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_buy_preview = AsyncMock(return_value=("Preview text", {"ticker": "AAPL", "action": "BUY"}))
        msg = _make_message(text="/buy AAPL 5%")
        await cmd_buy(msg)
        # First call is "Looking up", second is the preview
        assert msg.answer.await_count == 2

    async def test_buy_preview_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_buy_preview = AsyncMock(side_effect=RuntimeError("no contract"))
        msg = _make_message(text="/buy AAPL 5%")
        await cmd_buy(msg)
        last_text = msg.answer.call_args[0][0]
        assert "no contract" in last_text

    async def test_no_args_with_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[
            {"symbol": "AAPL", "total_qty": 10},
            {"symbol": "TSLA", "total_qty": 5},
        ])
        msg = _make_message(text="/buy")
        await cmd_buy(msg)
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        # Should have buttons for each position
        all_buttons = [b for row in keyboard.inline_keyboard for b in row]
        assert any("AAPL" in b.text for b in all_buttons)

    async def test_no_args_no_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[])
        msg = _make_message(text="/buy")
        await cmd_buy(msg)
        assert "No open positions" in msg.answer.call_args[0][0]

    async def test_no_args_not_connected(self, bot_instance):
        msg = _make_message(text="/buy")
        await cmd_buy(msg)
        assert "not connected" in msg.answer.call_args[0][0]


class TestCmdSell:
    async def test_with_args_and_preview(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_sell_preview = AsyncMock(return_value=("Sell preview", {"ticker": "AAPL", "action": "SELL"}))
        msg = _make_message(text="/sell AAPL all")
        await cmd_sell(msg)
        assert msg.answer.await_count == 2

    async def test_sell_preview_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_sell_preview = AsyncMock(side_effect=RuntimeError("no position"))
        msg = _make_message(text="/sell AAPL all")
        await cmd_sell(msg)
        last_text = msg.answer.call_args[0][0]
        assert "no position" in last_text

    async def test_no_args_with_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[
            {"symbol": "AAPL", "total_qty": 10},
        ])
        msg = _make_message(text="/sell")
        await cmd_sell(msg)
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_buttons = [b for row in keyboard.inline_keyboard for b in row]
        assert any("s:AAPL" in b.callback_data for b in all_buttons)

    async def test_no_args_no_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[])
        msg = _make_message(text="/sell")
        await cmd_sell(msg)
        assert "No open positions" in msg.answer.call_args[0][0]

    async def test_no_args_not_connected(self, bot_instance):
        msg = _make_message(text="/sell")
        await cmd_sell(msg)
        assert "not connected" in msg.answer.call_args[0][0]


class TestCmdNew:
    async def test_no_args_shows_force_reply(self, bot_instance):
        msg = _make_message(text="/new")
        await cmd_new(msg)
        kwargs = msg.answer.call_args[1]
        assert kwargs["reply_markup"].selective is True

    async def test_with_ticker(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_expiries = AsyncMock(return_value=[
            {"display": "05/24", "dte": 30, "exp": "20240524"},
        ])
        msg = _make_message(text="/new IREN")
        await cmd_new(msg)
        # First call: "Loading...", second: expiry picker
        assert msg.answer.await_count == 2

    async def test_no_expiries_callback(self, bot_instance):
        msg = _make_message(text="/new IREN")
        await cmd_new(msg)
        assert "not connected" in msg.answer.call_args[0][0]

    async def test_expiry_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_expiries = AsyncMock(side_effect=RuntimeError("timeout"))
        msg = _make_message(text="/new IREN")
        await cmd_new(msg)
        last_text = msg.answer.call_args[0][0]
        assert "timeout" in last_text


class TestCmdInfo:
    async def test_with_ticker(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_info = AsyncMock(return_value="<b>Info</b>")
        msg = _make_message(text="/info AAPL")
        await cmd_info(msg)
        bot_mod._on_info.assert_awaited_once_with("AAPL")

    async def test_no_ticker_with_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[{"symbol": "AAPL"}])
        msg = _make_message(text="/info")
        await cmd_info(msg)
        kwargs = msg.answer.call_args[1]
        assert kwargs["reply_markup"] is not None

    async def test_no_ticker_no_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[])
        msg = _make_message(text="/info")
        await cmd_info(msg)
        assert "No open positions" in msg.answer.call_args[0][0]

    async def test_no_ticker_no_callback(self, bot_instance):
        msg = _make_message(text="/info")
        await cmd_info(msg)
        assert "Usage" in msg.answer.call_args[0][0]

    async def test_info_not_connected(self, bot_instance):
        msg = _make_message(text="/info AAPL")
        await cmd_info(msg)
        assert "not connected" in msg.answer.call_args[0][0]

    async def test_info_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_info = AsyncMock(side_effect=ValueError("bad ticker"))
        msg = _make_message(text="/info XYZ")
        await cmd_info(msg)
        assert "bad ticker" in msg.answer.call_args[0][0]


class TestCmdPrice:
    async def test_with_ticker(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_price = AsyncMock(return_value="$150.00")
        msg = _make_message(text="/price AAPL")
        await cmd_price(msg)
        bot_mod._on_price.assert_awaited_once_with("AAPL")

    async def test_no_ticker_with_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[{"symbol": "AAPL"}])
        msg = _make_message(text="/price")
        await cmd_price(msg)
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        assert any("p:AAPL" in b.callback_data for row in keyboard.inline_keyboard for b in row)

    async def test_no_ticker_no_positions(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_list_positions = AsyncMock(return_value=[])
        msg = _make_message(text="/price")
        await cmd_price(msg)
        assert "No open positions" in msg.answer.call_args[0][0]

    async def test_no_ticker_no_callback(self, bot_instance):
        msg = _make_message(text="/price")
        await cmd_price(msg)
        assert "Usage" in msg.answer.call_args[0][0]

    async def test_price_not_connected(self, bot_instance):
        msg = _make_message(text="/price AAPL")
        await cmd_price(msg)
        assert "not connected" in msg.answer.call_args[0][0]

    async def test_price_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_price = AsyncMock(side_effect=ValueError("unknown"))
        msg = _make_message(text="/price XYZ")
        await cmd_price(msg)
        assert "unknown" in msg.answer.call_args[0][0]


class TestCmdOrders:
    async def test_with_orders(self, bot_instance):
        import src.bot as bot_mod

        orders = [
            {"order_id": 1, "action": "BUY", "qty": 10, "symbol": "AAPL", "account_name": "live"},
            {"order_id": 2, "action": "SELL", "qty": 5, "symbol": "TSLA", "account_name": "live"},
        ]
        bot_mod._on_orders = AsyncMock(return_value=("Orders:", orders))
        msg = _make_message(text="/orders")
        await cmd_orders(msg)
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        # cancel buttons + cancel all + refresh
        all_buttons = [b for row in keyboard.inline_keyboard for b in row]
        assert any("cx:all" in b.callback_data for b in all_buttons)
        assert any("cx:refresh" in b.callback_data for b in all_buttons)

    async def test_no_orders(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_orders = AsyncMock(return_value=("No open orders", []))
        msg = _make_message(text="/orders")
        await cmd_orders(msg)
        assert "No open orders" in msg.answer.call_args[0][0]

    async def test_not_connected(self, bot_instance):
        msg = _make_message(text="/orders")
        await cmd_orders(msg)
        assert "not connected" in msg.answer.call_args[0][0]

    async def test_single_order_no_cancel_all(self, bot_instance):
        import src.bot as bot_mod

        orders = [{"order_id": 1, "action": "BUY", "qty": 10, "symbol": "AAPL", "account_name": "live"}]
        bot_mod._on_orders = AsyncMock(return_value=("Orders:", orders))
        msg = _make_message(text="/orders")
        await cmd_orders(msg)
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        # Single order should NOT have "cancel all"
        assert "cx:all" not in all_data


class TestCmdTrades:
    async def test_shows_today(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_trades = AsyncMock(return_value="Trades today")
        msg = _make_message(text="/trades")
        await cmd_trades(msg)
        bot_mod._on_trades.assert_awaited_once_with("today")
        kwargs = msg.answer.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_buttons = [b for row in keyboard.inline_keyboard for b in row]
        assert any("trades:today" in b.callback_data for b in all_buttons)
        assert any("trades:week" in b.callback_data for b in all_buttons)

    async def test_not_connected(self, bot_instance):
        msg = _make_message(text="/trades")
        await cmd_trades(msg)
        assert "not connected" in msg.answer.call_args[0][0]

    async def test_trades_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_trades = AsyncMock(side_effect=RuntimeError("fail"))
        msg = _make_message(text="/trades")
        await cmd_trades(msg)
        assert "fail" in msg.answer.call_args[0][0]


# ── Callback handlers ───────────────────────────────────────────────────────


class TestOnExecute:
    async def test_execute_with_on_confirm(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 42, "signal": signal, "_created_at": 0}

        result = MagicMock()
        result.success = True
        result.account_name = "live"
        result.filled_qty = 10
        result.avg_price = 150.50
        result.error = None
        bot_mod._on_confirm = AsyncMock(return_value=[result])

        cb = _make_callback(data="exec:key1")
        await on_execute(cb)

        bot_mod._on_confirm.assert_awaited_once_with(42, signal)
        cb.message.edit_text.assert_awaited_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "EXECUTED" in text
        assert "live" in text
        assert "key1" not in bot_mod._pending

    async def test_execute_error(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 1, "signal": signal, "_created_at": 0}
        bot_mod._on_confirm = AsyncMock(side_effect=RuntimeError("execution failed"))

        cb = _make_callback(data="exec:key1")
        await on_execute(cb)

        text = cb.message.edit_text.call_args[0][0]
        assert "EXECUTION FAILED" in text

    async def test_execute_no_confirm_callback(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 1, "signal": signal, "_created_at": 0}

        cb = _make_callback(data="exec:key1")
        await on_execute(cb)

        text = cb.message.edit_text.call_args[0][0]
        assert "CONFIRMED" in text
        assert "executor not connected" in text

    async def test_execute_expired(self, bot_instance):
        cb = _make_callback(data="exec:nonexistent")
        await on_execute(cb)
        cb.answer.assert_awaited_once_with("Signal expired or already processed")

    async def test_dedup_prevents_double_execute(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 1, "signal": signal, "_created_at": 0}

        cb1 = _make_callback(data="exec:key1")
        await on_execute(cb1)

        # Second execution of the same key
        cb2 = _make_callback(data="exec:key1")
        await on_execute(cb2)
        cb2.answer.assert_awaited_once_with("Signal already submitted", show_alert=True)


class TestOnSkip:
    async def test_skip_removes_pending(self, bot_instance):
        import src.bot as bot_mod

        signal = TradeSignal(ticker="AAPL", action="BUY")
        bot_mod._pending["key1"] = {"signal_id": 1, "signal": signal, "_created_at": 0}

        cb = _make_callback(data="skip:key1")
        await on_skip(cb)

        assert "key1" not in bot_mod._pending
        cb.answer.assert_awaited_once_with("Trade skipped")
        text = cb.message.edit_text.call_args[0][0]
        assert "SKIPPED" in text

    async def test_skip_nonexistent_key(self, bot_instance):
        """Skipping a key that doesn't exist should not raise."""
        cb = _make_callback(data="skip:nonexistent")
        await on_skip(cb)
        cb.answer.assert_awaited_once_with("Trade skipped")


class TestOnKillConfirm:
    async def test_kills_orders(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_kill = AsyncMock(return_value="Cancelled 3 orders")
        cb = _make_callback(data="kill:confirm")
        await on_kill_confirm(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "Cancelled 3 orders" in text

    async def test_kill_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_kill = AsyncMock(side_effect=RuntimeError("fail"))
        cb = _make_callback(data="kill:confirm")
        await on_kill_confirm(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "fail" in text

    async def test_kill_not_connected(self, bot_instance):
        cb = _make_callback(data="kill:confirm")
        await on_kill_confirm(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "not connected" in text


class TestOnKillCancel:
    async def test_cancels(self, bot_instance):
        cb = _make_callback(data="kill:cancel")
        await on_kill_cancel(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "cancelled" in text


class TestOnValueToggle:
    async def test_day_mode(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_value = AsyncMock(return_value="Day view")
        cb = _make_callback(data="v:day")
        await on_value_toggle(cb)
        bot_mod._on_value.assert_awaited_once_with("day")
        kwargs = cb.message.edit_text.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        assert "v:total" in all_data

    async def test_total_mode(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_value = AsyncMock(return_value="Total view")
        cb = _make_callback(data="v:total")
        await on_value_toggle(cb)
        kwargs = cb.message.edit_text.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        assert "v:day" in all_data

    async def test_help_mode(self, bot_instance):
        cb = _make_callback(data="v:help")
        await on_value_toggle(cb)
        cb.message.answer.assert_awaited_once_with(VHELP_TEXT, parse_mode="HTML")

    async def test_not_connected(self, bot_instance):
        cb = _make_callback(data="v:day")
        await on_value_toggle(cb)
        cb.answer.assert_any_await("Not connected")

    async def test_value_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_value = AsyncMock(side_effect=RuntimeError("timeout"))
        cb = _make_callback(data="v:day")
        await on_value_toggle(cb)
        cb.answer.assert_any_await("Error: timeout", show_alert=True)


class TestOnPauseCallback:
    async def test_cancel(self, bot_instance):
        cb = _make_callback(data="pause:cancel")
        await on_pause_callback(cb)
        cb.answer.assert_awaited_once_with("Cancelled")
        text = cb.message.edit_text.call_args[0][0]
        assert "cancelled" in text

    async def test_resume(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_resume = AsyncMock(return_value="Gateways resumed")
        cb = _make_callback(data="pause:resume")
        await on_pause_callback(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "Gateways resumed" in text

    async def test_pause_minutes(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_pause = AsyncMock(return_value="Paused for 15 min")
        cb = _make_callback(data="pause:15")
        await on_pause_callback(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "Paused for 15 min" in text
        # Should have Resume button
        kwargs = cb.message.edit_text.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        assert "pause:resume" in all_data

    async def test_invalid_minutes(self, bot_instance):
        cb = _make_callback(data="pause:abc")
        await on_pause_callback(cb)
        cb.answer.assert_awaited_once_with("Invalid")


class TestOnTradesPeriod:
    async def test_switches_period(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_trades = AsyncMock(return_value="Week trades")
        cb = _make_callback(data="trades:week")
        await on_trades_period(cb)
        bot_mod._on_trades.assert_awaited_once_with("week")

    async def test_not_connected(self, bot_instance):
        cb = _make_callback(data="trades:today")
        await on_trades_period(cb)
        cb.answer.assert_any_await("Not connected", show_alert=True)

    async def test_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_trades = AsyncMock(side_effect=RuntimeError("fail"))
        cb = _make_callback(data="trades:week")
        await on_trades_period(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "fail" in text


class TestOnOrdersAction:
    async def test_refresh(self, bot_instance):
        import src.bot as bot_mod

        orders = [{"order_id": 1, "action": "BUY", "qty": 5, "symbol": "AAPL", "account_name": "live"}]
        bot_mod._on_orders = AsyncMock(return_value=("Refreshed", orders))
        cb = _make_callback(data="cx:refresh")
        await on_orders_action(cb)
        cb.message.edit_text.assert_awaited_once()

    async def test_refresh_empty(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_orders = AsyncMock(return_value=("No orders", []))
        cb = _make_callback(data="cx:refresh")
        await on_orders_action(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "No orders" in text

    async def test_refresh_not_connected(self, bot_instance):
        cb = _make_callback(data="cx:refresh")
        await on_orders_action(cb)
        cb.answer.assert_any_await("Not connected")

    async def test_cancel_all(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_cancel_all = AsyncMock(return_value="All cancelled")
        cb = _make_callback(data="cx:all")
        await on_orders_action(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "All cancelled" in text

    async def test_cancel_all_not_connected(self, bot_instance):
        cb = _make_callback(data="cx:all")
        await on_orders_action(cb)
        cb.answer.assert_any_await("Not connected")

    async def test_cancel_single_order(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_cancel_order = AsyncMock(return_value="Cancelled #123")
        cb = _make_callback(data="cx:live:123")
        await on_orders_action(cb)
        bot_mod._on_cancel_order.assert_awaited_once_with("live", 123)

    async def test_cancel_invalid_order_id(self, bot_instance):
        cb = _make_callback(data="cx:live:abc")
        await on_orders_action(cb)
        cb.answer.assert_any_await("Invalid order ID")

    async def test_cancel_invalid_format(self, bot_instance):
        cb = _make_callback(data="cx:nocolon")
        await on_orders_action(cb)
        # "nocolon" has no ":" so rsplit gives 1 element -> "Invalid action"
        cb.answer.assert_any_await("Invalid action")

    async def test_cancel_order_not_connected(self, bot_instance):
        cb = _make_callback(data="cx:live:123")
        await on_orders_action(cb)
        cb.answer.assert_any_await("Not connected")


class TestOnOrderExecute:
    async def test_buy_execute(self, bot_instance):
        import src.bot as bot_mod

        order_details = {"action": "BUY", "ticker": "AAPL", "_created_at": _time.time()}
        bot_mod._pending_orders["buy_AAPL_1"] = order_details
        bot_mod._on_buy_execute = AsyncMock(return_value="Filled 10 @ 150")

        cb = _make_callback(data="order:exec:buy_AAPL_1")
        await on_order_execute(cb)

        bot_mod._on_buy_execute.assert_awaited_once()
        text = cb.message.edit_text.call_args[0][0]
        assert "ORDERS PLACED" in text

    async def test_sell_execute(self, bot_instance):
        import src.bot as bot_mod

        order_details = {"action": "SELL", "ticker": "AAPL", "_created_at": _time.time()}
        bot_mod._pending_orders["sell_AAPL_1"] = order_details
        bot_mod._on_sell_execute = AsyncMock(return_value="Sold 5 @ 155")

        cb = _make_callback(data="order:exec:sell_AAPL_1")
        await on_order_execute(cb)

        bot_mod._on_sell_execute.assert_awaited_once()

    async def test_expired_order(self, bot_instance):
        cb = _make_callback(data="order:exec:nonexistent")
        await on_order_execute(cb)
        cb.answer.assert_awaited_once_with("Order expired or already processed")

    async def test_dedup(self, bot_instance):
        import src.bot as bot_mod

        order_details = {"action": "BUY", "ticker": "AAPL", "_created_at": _time.time()}
        bot_mod._pending_orders["buy_1"] = order_details
        bot_mod._on_buy_execute = AsyncMock(return_value="ok")

        cb1 = _make_callback(data="order:exec:buy_1")
        await on_order_execute(cb1)

        cb2 = _make_callback(data="order:exec:buy_1")
        await on_order_execute(cb2)
        cb2.answer.assert_awaited_once_with("Order already submitted")

    async def test_execution_error(self, bot_instance):
        import src.bot as bot_mod

        order_details = {"action": "BUY", "ticker": "AAPL", "_created_at": _time.time()}
        bot_mod._pending_orders["buy_1"] = order_details
        bot_mod._on_buy_execute = AsyncMock(side_effect=RuntimeError("network"))

        cb = _make_callback(data="order:exec:buy_1")
        await on_order_execute(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "FAILED" in text

    async def test_no_executor(self, bot_instance):
        import src.bot as bot_mod

        order_details = {"action": "BUY", "ticker": "AAPL", "_created_at": _time.time()}
        bot_mod._pending_orders["buy_1"] = order_details

        cb = _make_callback(data="order:exec:buy_1")
        await on_order_execute(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "Executor not connected" in text


class TestOnOrderCancel:
    async def test_cancels_pending(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._pending_orders["key1"] = {"ticker": "AAPL", "_created_at": _time.time()}

        cb = _make_callback(data="order:cancel:key1")
        await on_order_cancel(cb)

        assert "key1" not in bot_mod._pending_orders
        text = cb.message.edit_text.call_args[0][0]
        assert "CANCELLED" in text
        cb.answer.assert_awaited_once_with("Order cancelled")


class TestOnBuyPick:
    async def test_ticker_only_shows_pct_picker(self, bot_instance):
        cb = _make_callback(data="a:IREN")
        await on_buy_pick(cb)
        kwargs = cb.message.edit_text.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        assert "a:IREN:5" in all_data
        assert "a:IREN:10" in all_data

    async def test_ticker_with_pct_previews(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_buy_preview = AsyncMock(return_value=("Preview", {"ticker": "IREN", "action": "BUY"}))
        cb = _make_callback(data="a:IREN:5")
        await on_buy_pick(cb)
        bot_mod._on_buy_preview.assert_awaited_once_with("IREN 5 mkt")

    async def test_ticker_with_pct_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_buy_preview = AsyncMock(side_effect=RuntimeError("fail"))
        cb = _make_callback(data="a:IREN:5")
        await on_buy_pick(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "fail" in text


class TestOnSellPick:
    async def test_ticker_only_shows_fraction_picker(self, bot_instance):
        cb = _make_callback(data="s:IREN")
        await on_sell_pick(cb)
        kwargs = cb.message.edit_text.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        assert "s:IREN:all" in all_data
        assert "s:IREN:half" in all_data

    async def test_ticker_with_fraction_previews(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_sell_preview = AsyncMock(return_value=("Preview", {"ticker": "IREN", "action": "SELL"}))
        cb = _make_callback(data="s:IREN:all")
        await on_sell_pick(cb)
        bot_mod._on_sell_preview.assert_awaited_once_with("IREN all mkt")


class TestOnInfoPick:
    async def test_loads_info(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_info = AsyncMock(return_value="<b>AAPL details</b>")
        cb = _make_callback(data="i:AAPL")
        await on_info_pick(cb)
        bot_mod._on_info.assert_awaited_once_with("AAPL")

    async def test_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_info = AsyncMock(side_effect=ValueError("err"))
        cb = _make_callback(data="i:XYZ")
        await on_info_pick(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "err" in text


class TestOnPricePick:
    async def test_loads_price(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_price = AsyncMock(return_value="$150")
        cb = _make_callback(data="p:AAPL")
        await on_price_pick(cb)
        bot_mod._on_price.assert_awaited_once_with("AAPL")

    async def test_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_price = AsyncMock(side_effect=ValueError("err"))
        cb = _make_callback(data="p:XYZ")
        await on_price_pick(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "err" in text


class TestOnNewChain:
    async def test_switch_to_calls(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_expiries = AsyncMock(return_value=[
            {"display": "05/24", "dte": 30, "exp": "20240524"},
        ])
        cb = _make_callback(data="n:IREN:C:exp")
        await on_new_chain(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "calls" in text

    async def test_switch_to_puts(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_expiries = AsyncMock(return_value=[
            {"display": "05/24", "dte": 30, "exp": "20240524"},
        ])
        cb = _make_callback(data="n:IREN:P:exp")
        await on_new_chain(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "puts" in text

    async def test_show_strikes(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_strikes = AsyncMock(return_value={
            "strikes": [{"strike": 85.0, "label": "OTM"}],
            "current_price": 80.50,
        })
        cb = _make_callback(data="n:IREN:20240524:C")
        await on_new_chain(cb)
        text = cb.message.edit_text.call_args[0][0]
        assert "IREN" in text
        assert "$80.50" in text

    async def test_show_pct_picker(self, bot_instance):
        cb = _make_callback(data="n:IREN:20240524:C:85")
        await on_new_chain(cb)
        kwargs = cb.message.edit_text.call_args[1]
        keyboard = kwargs["reply_markup"]
        all_data = [b.callback_data for row in keyboard.inline_keyboard for b in row]
        assert any(":5" in d for d in all_data)

    async def test_final_preview(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_buy_preview = AsyncMock(return_value=("Order preview", {"ticker": "IREN", "action": "BUY"}))
        cb = _make_callback(data="n:IREN:20240524:C:85:5")
        await on_new_chain(cb)
        # Should have called _on_buy_preview with formatted args
        bot_mod._on_buy_preview.assert_awaited_once()
        args_str = bot_mod._on_buy_preview.call_args[0][0]
        assert "IREN" in args_str
        assert "MAY24" in args_str

    async def test_unknown_action(self, bot_instance):
        cb = _make_callback(data="n:IREN:20240524:C:85:5:extra:garbage")
        await on_new_chain(cb)
        cb.answer.assert_any_await("Unknown action")

    async def test_expiries_not_connected(self, bot_instance):
        cb = _make_callback(data="n:IREN:C:exp")
        await on_new_chain(cb)
        cb.answer.assert_any_await("Not connected", show_alert=True)

    async def test_strikes_not_connected(self, bot_instance):
        cb = _make_callback(data="n:IREN:20240524:C")
        await on_new_chain(cb)
        cb.answer.assert_any_await("Not connected", show_alert=True)


class TestHandleNewTickerReply:
    async def test_processes_reply(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_expiries = AsyncMock(return_value=[
            {"display": "05/24", "dte": 30, "exp": "20240524"},
        ])
        msg = _make_message(text="IREN")
        msg.reply_to_message = MagicMock()
        msg.reply_to_message.text = "Type the ticker symbol"
        await handle_new_ticker_reply(msg)
        assert msg.answer.await_count == 2

    async def test_non_admin(self, bot_instance):
        msg = _make_message(user_id=1, text="IREN")
        await handle_new_ticker_reply(msg)
        msg.answer.assert_not_awaited()

    async def test_empty_ticker(self, bot_instance):
        msg = _make_message(text="   ")
        await handle_new_ticker_reply(msg)
        msg.answer.assert_not_awaited()

    async def test_not_connected(self, bot_instance):
        msg = _make_message(text="IREN")
        await handle_new_ticker_reply(msg)
        assert "not connected" in msg.answer.call_args[0][0]

    async def test_error(self, bot_instance):
        import src.bot as bot_mod

        bot_mod._on_option_expiries = AsyncMock(side_effect=RuntimeError("timeout"))
        msg = _make_message(text="IREN")
        await handle_new_ticker_reply(msg)
        last_text = msg.answer.call_args[0][0]
        assert "timeout" in last_text
