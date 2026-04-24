"""Tests for the App orchestrator — all external dependencies mocked."""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AccountConfig, Config, TradingConfig
from src.executor import ExecutionResult
from src.models import TradeSignal

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_config(**overrides):
    """Build a minimal Config for testing."""
    defaults = dict(
        bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        admin_chat_id=12345,
        accounts=[
            AccountConfig(
                name="test-acct",
                gateway_host="gw-test",
                gateway_port=4003,
                display_name="Test",
                net_deposits=10_000,
                max_position_pct=15.0,
                max_allocation_pct=100.0,
            ),
        ],
        trading=TradingConfig(),
        webhook_secret="test-secret",
        webhook_port=0,
        db_path=":memory:",
    )
    defaults.update(overrides)
    return Config(**defaults)


def _make_signal(**overrides):
    """Build a minimal TradeSignal for testing."""
    defaults = dict(
        ticker="AAPL",
        action="BUY",
        target_weight_pct=5.0,
        raw_text="Buy AAPL 5%",
        source="webhook",
    )
    defaults.update(overrides)
    return TradeSignal(**defaults)


def _build_app(config=None):
    """Create an App with all external deps mocked at their import locations."""
    config = config or _make_config()

    with (
        patch("src.app.Database") as mock_db_cls,
        patch("src.app.TradeExecutor") as mock_executor_cls,
        patch("src.app.ConfirmationBot") as mock_bot_cls,
        patch("src.app.WebhookServer") as mock_webhook_cls,
    ):
        db = mock_db_cls.return_value
        db.init = AsyncMock()
        db.close = AsyncMock()
        db.save_signal = AsyncMock(return_value=1)
        db.log_audit = AsyncMock()
        db.update_signal_status = AsyncMock()
        db.save_execution = AsyncMock()
        db.get_positions = AsyncMock(return_value=[])
        db.get_recent_signals = AsyncMock(return_value=[])
        db.get_last_sync_time = AsyncMock(return_value="2026-04-20T10:00:00")
        db.find_recent_signal = AsyncMock(return_value=None)
        db.get_cash_transactions = AsyncMock(return_value=[])
        db.get_net_deposits = AsyncMock(return_value=0.0)
        db.upsert_cash_transaction = AsyncMock()
        db.update_account_deposits = AsyncMock()
        db.upsert_position = AsyncMock()
        db.delete_stale_positions = AsyncMock(return_value=0)
        db.upsert_account_summary = AsyncMock()
        db.find_execution_by_order = AsyncMock(return_value=None)
        db.update_execution_fill = AsyncMock()
        db.update_execution_allocation = AsyncMock()
        db.get_executions_since = AsyncMock(return_value=[])

        executor = mock_executor_cls.return_value
        executor.connectors = {}
        executor.connect_all = AsyncMock()
        executor.disconnect_all = AsyncMock()
        executor.execute = AsyncMock(return_value=[])

        bot = mock_bot_cls.return_value
        bot.send_confirmation = AsyncMock()
        bot.send_notification = AsyncMock()
        bot.start = AsyncMock()
        bot.stop = AsyncMock()
        bot.get_pending_count = MagicMock(return_value=0)

        webhook = mock_webhook_cls.return_value
        webhook.start = AsyncMock()
        webhook.stop = AsyncMock()

        from src.app import App

        app = App(config)

    return app


def _make_connector_mock(name="test-acct", nlv=100_000.0, positions=None, portfolio=None):
    """Create a mock IBKRConnector with sensible defaults."""
    conn = MagicMock()
    conn.is_connected = True
    conn.get_nlv = AsyncMock(return_value=nlv)
    conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": nlv, "USD": nlv * 1.1})
    conn.get_positions = AsyncMock(return_value=positions or [])
    conn.get_portfolio = AsyncMock(return_value=portfolio or [])
    conn.get_daily_pnl = AsyncMock(return_value={"dailyPnL": 150.0})
    conn.get_positions_daily_pnl = AsyncMock(return_value={})
    conn.get_exchange_rate = AsyncMock(return_value=0.88)
    conn.get_cash_balances = AsyncMock(return_value={"EUR": 5000.0, "USD": 3000.0})
    conn.cancel_all_orders = AsyncMock(return_value=3)
    conn.subscribe_pnl = AsyncMock()
    conn.get_margin_used = AsyncMock(return_value=0.0)
    conn.get_open_orders = MagicMock(return_value=[])
    conn.get_trades = MagicMock(return_value=[])
    conn.get_stock_prices_batch = AsyncMock(return_value={})
    conn.get_option_data_batch = AsyncMock(return_value={})
    conn.get_available_funds = AsyncMock(return_value=50_000.0)
    conn.cancel_order = AsyncMock(return_value=True)
    conn.managed_accounts = MagicMock(return_value=["U1234567"])
    conn.req_account_updates = MagicMock()
    return conn


# ── App.__init__ ─────────────────────────────────────────────────────────────


class TestAppInit:
    def test_webhook_created_when_secret_set(self):
        app = _build_app(_make_config(webhook_secret="my-secret"))
        assert app.webhook is not None

    def test_webhook_not_created_when_secret_empty(self):
        app = _build_app(_make_config(webhook_secret=""))
        assert app.webhook is None

    def test_deposit_baselines_captured(self):
        cfg = _make_config()
        app = _build_app(cfg)
        assert app._deposit_baselines == {"test-acct": 10_000}

    def test_gateway_containers_from_config(self):
        app = _build_app()
        assert app._gateway_containers == ["gw-test"]

    def test_gateway_not_paused_initially(self):
        app = _build_app()
        assert app._gateway_paused is False


# ── App.shutdown() ───────────────────────────────────────────────────────────


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_stops_all_components(self):
        app = _build_app()
        await app.shutdown()

        app.webhook.stop.assert_awaited_once()
        app.bot.stop.assert_awaited_once()
        app.executor.disconnect_all.assert_awaited_once()
        app.db.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_skips_webhook_when_none(self):
        app = _build_app(_make_config(webhook_secret=""))
        await app.shutdown()

        # No webhook to stop, but other components still stopped
        app.bot.stop.assert_awaited_once()
        app.executor.disconnect_all.assert_awaited_once()
        app.db.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_skips_bot_stop_when_no_token(self):
        app = _build_app(_make_config(bot_token=""))
        await app.shutdown()

        app.bot.stop.assert_not_awaited()
        app.executor.disconnect_all.assert_awaited_once()
        app.db.close.assert_awaited_once()


# ── _on_webhook_signal() ────────────────────────────────────────────────────


class TestOnWebhookSignal:
    @pytest.mark.asyncio
    async def test_new_signal_saved_and_confirmation_sent(self):
        app = _build_app()
        signal = _make_signal()

        with patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False):
            result = await app._on_webhook_signal(signal)

        assert result["status"] == "pending_confirmation"
        assert result["signal_id"] == 1
        app.db.save_signal.assert_awaited_once()
        app.db.log_audit.assert_awaited_once()
        app.bot.send_confirmation.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_duplicate_signal_skipped(self):
        app = _build_app()
        signal = _make_signal()

        with patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=True):
            result = await app._on_webhook_signal(signal)

        assert result["status"] == "duplicate_skipped"
        app.db.save_signal.assert_not_awaited()
        app.bot.send_confirmation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_bot_returns_saved_no_bot(self):
        app = _build_app(_make_config(bot_token=""))
        signal = _make_signal()

        with patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False):
            result = await app._on_webhook_signal(signal)

        assert result["status"] == "saved_no_bot"
        app.db.save_signal.assert_awaited_once()
        app.bot.send_confirmation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_signal_fields_passed_to_db(self):
        app = _build_app()
        signal = _make_signal(
            ticker="MSFT", action="SELL", target_weight_pct=10.0,
            amount_description="all", raw_text="Sell MSFT all",
            source="api", message_id=99,
        )

        with patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False):
            await app._on_webhook_signal(signal)

        call_kwargs = app.db.save_signal.call_args.kwargs
        assert call_kwargs["ticker"] == "MSFT"
        assert call_kwargs["action"] == "SELL"
        assert call_kwargs["target_weight_pct"] == 10.0
        assert call_kwargs["source"] == "api"
        assert call_kwargs["message_id"] == 99


# ── _on_trade_confirmed() ───────────────────────────────────────────────────


class TestOnTradeConfirmed:
    @pytest.mark.asyncio
    async def test_market_closed_returns_skipped(self):
        app = _build_app()
        signal = _make_signal()

        with (
            patch("src.app.is_market_open", return_value=False),
            patch("src.app.time_until_market_open", return_value=timedelta(hours=14)),
        ):
            results = await app._on_trade_confirmed(1, signal)

        assert len(results) == 1
        assert results[0].success is False
        assert "Market closed" in results[0].error
        app.db.update_signal_status.assert_any_await(1, "skipped")

    @pytest.mark.asyncio
    async def test_duplicate_returns_skipped(self):
        app = _build_app()
        signal = _make_signal()

        with (
            patch("src.app.is_market_open", return_value=True),
            patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=True),
        ):
            results = await app._on_trade_confirmed(1, signal)

        assert len(results) == 1
        assert results[0].success is False
        assert "Duplicate" in results[0].error

    @pytest.mark.asyncio
    async def test_position_limit_excludes_account(self):
        app = _build_app()
        signal = _make_signal()
        app.executor.execute = AsyncMock(return_value=[
            ExecutionResult(account_name="test-acct", success=True, order_id=100),
        ])

        with (
            patch("src.app.is_market_open", return_value=True),
            patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False),
            patch("src.app.check_position_limits", return_value="Exceeds max position"),
        ):
            await app._on_trade_confirmed(1, signal)

        # execute was called with the blocked account in exclude_accounts
        call_kwargs = app.executor.execute.call_args.kwargs
        assert "test-acct" in call_kwargs["exclude_accounts"]

    @pytest.mark.asyncio
    async def test_successful_execution_saved(self):
        app = _build_app()
        signal = _make_signal()
        app.executor.execute = AsyncMock(return_value=[
            ExecutionResult(account_name="test-acct", success=True, order_id=42),
        ])

        with (
            patch("src.app.is_market_open", return_value=True),
            patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False),
            patch("src.app.check_position_limits", return_value=None),
        ):
            results = await app._on_trade_confirmed(1, signal)

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].order_id == 42
        app.db.save_execution.assert_awaited_once()
        # Signal status updated to "executed"
        app.db.update_signal_status.assert_any_await(1, "executed")

    @pytest.mark.asyncio
    async def test_failed_execution_saved(self):
        app = _build_app()
        signal = _make_signal()
        app.executor.execute = AsyncMock(return_value=[
            ExecutionResult(account_name="test-acct", success=False, error="Gateway down"),
        ])

        with (
            patch("src.app.is_market_open", return_value=True),
            patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False),
            patch("src.app.check_position_limits", return_value=None),
        ):
            results = await app._on_trade_confirmed(1, signal)

        assert results[0].success is False
        # Signal status updated to "failed"
        app.db.update_signal_status.assert_any_await(1, "failed")

    @pytest.mark.asyncio
    async def test_multiple_accounts_partial_block(self):
        """One account blocked by limits, the other executes fine."""
        cfg = _make_config(accounts=[
            AccountConfig(name="acct-a", gateway_host="gw-a", gateway_port=4003,
                          display_name="A", max_position_pct=15.0, max_allocation_pct=100.0),
            AccountConfig(name="acct-b", gateway_host="gw-b", gateway_port=4004,
                          display_name="B", max_position_pct=15.0, max_allocation_pct=100.0),
        ])
        app = _build_app(cfg)
        signal = _make_signal()
        app.executor.execute = AsyncMock(return_value=[
            ExecutionResult(account_name="acct-b", success=True, order_id=7),
        ])

        # acct-a blocked, acct-b clear
        def limit_check(action, ticker, target, max_pos, max_alloc, positions):
            if True:  # just use call count
                return None
            return None

        call_count = 0

        def side_effect_limits(action, ticker, target, max_pos, max_alloc, positions):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "Exceeds max"
            return None

        with (
            patch("src.app.is_market_open", return_value=True),
            patch("src.app.check_duplicate_signal", new_callable=AsyncMock, return_value=False),
            patch("src.app.check_position_limits", side_effect=side_effect_limits),
        ):
            await app._on_trade_confirmed(1, signal)

        call_kwargs = app.executor.execute.call_args.kwargs
        assert "acct-a" in call_kwargs["exclude_accounts"]


# ── _on_positions_requested() ────────────────────────────────────────────────


class TestOnPositionsRequested:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_positions_requested()
        assert result == "No IBKR accounts connected"

    @pytest.mark.asyncio
    async def test_with_connectors_no_positions(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_positions_requested()
        assert "test-acct" in result
        assert "No positions" in result

    @pytest.mark.asyncio
    async def test_with_connectors_and_positions(self):
        app = _build_app()
        pos = MagicMock()
        pos.contract = MagicMock()
        pos.contract.symbol = "AAPL"
        pos.position = 10
        conn = _make_connector_mock(positions=[pos])
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_positions_requested()
        assert "AAPL" in result
        assert "10" in result


# ── _on_kill_requested() ────────────────────────────────────────────────────


class TestOnKillRequested:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_kill_requested()
        assert result == "No IBKR accounts connected"

    @pytest.mark.asyncio
    async def test_cancels_orders(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_kill_requested()
        conn.cancel_all_orders.assert_awaited_once()
        assert "cancelled 3 orders" in result

    @pytest.mark.asyncio
    async def test_handles_error(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.cancel_all_orders = AsyncMock(side_effect=RuntimeError("connection lost"))
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_kill_requested()
        assert "ERROR" in result
        assert "connection lost" in result


# ── _on_portfolio_requested() ────────────────────────────────────────────────


class TestOnPortfolioRequested:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_portfolio_requested()
        assert result == "No IBKR accounts connected"

    @pytest.mark.asyncio
    async def test_with_connector_shows_nlv_and_returns(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=50_000.0)
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_portfolio_requested()
        assert "Portfolio Summary" in result
        assert "50,000" in result
        assert "Combined" in result

    @pytest.mark.asyncio
    async def test_disconnected_account_shown(self):
        app = _build_app()
        app.executor.connectors = {}  # no connector for account

        result = await app._on_portfolio_requested()
        # Falls through to "No IBKR accounts connected"
        assert "No IBKR accounts connected" in result


# ── _on_signals_requested() ─────────────────────────────────────────────────


class TestOnSignalsRequested:
    @pytest.mark.asyncio
    async def test_no_signals(self):
        app = _build_app()
        app.db.get_recent_signals = AsyncMock(return_value=[])
        result = await app._on_signals_requested()
        assert result == "No signals recorded yet."

    @pytest.mark.asyncio
    async def test_formats_signals(self):
        app = _build_app()
        app.db.get_recent_signals = AsyncMock(return_value=[
            {"ticker": "AAPL", "action": "BUY", "status": "executed", "created_at": "2026-04-20T10:00:00"},
            {"ticker": "MSFT", "action": "SELL", "status": "pending", "created_at": "2026-04-20T11:00:00"},
        ])
        result = await app._on_signals_requested()
        assert "AAPL" in result
        assert "MSFT" in result
        assert "BUY" in result
        assert "SELL" in result

    @pytest.mark.asyncio
    async def test_passes_limit(self):
        app = _build_app()
        app.db.get_recent_signals = AsyncMock(return_value=[])
        await app._on_signals_requested(limit=5)
        app.db.get_recent_signals.assert_awaited_once_with(5)


# ── _on_health_requested() ──────────────────────────────────────────────────


class TestOnHealthRequested:
    @pytest.mark.asyncio
    async def test_all_connected(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_health_requested()
        assert "System Health" in result
        assert "Test" in result  # display_name
        assert "Telegram bot: active" in result
        assert "Pending confirmations: 0" in result

    @pytest.mark.asyncio
    async def test_disconnected_gateway(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.is_connected = False
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_health_requested()
        assert "Disconnected" in result

    @pytest.mark.asyncio
    async def test_last_sync_shown(self):
        app = _build_app()
        app.executor.connectors = {}
        app.db.get_last_sync_time = AsyncMock(return_value="2026-04-20T10:30:00")

        result = await app._on_health_requested()
        assert "Last sync" in result
        assert "2026-04-20T10:30:0" in result

    @pytest.mark.asyncio
    async def test_last_sync_exception_handled(self):
        app = _build_app()
        app.executor.connectors = {}
        app.db.get_last_sync_time = AsyncMock(side_effect=Exception("db error"))

        # Should not raise
        result = await app._on_health_requested()
        assert "System Health" in result

    @pytest.mark.asyncio
    async def test_webhook_listening_no_signals(self):
        app = _build_app()
        app.executor.connectors = {}
        app.webhook.last_signal_at = None
        app.webhook.total_signals = 0

        result = await app._on_health_requested()
        assert "Webhook: listening (no signals yet)" in result

    @pytest.mark.asyncio
    async def test_webhook_disabled(self):
        app = _build_app()
        app.executor.connectors = {}
        app.webhook = None

        result = await app._on_health_requested()
        assert "Webhook: disabled" in result

    @pytest.mark.asyncio
    async def test_web_url_shown(self):
        cfg = _make_config(web_url="https://dash.example.com")
        app = _build_app(config=cfg)
        app.executor.connectors = {}

        result = await app._on_health_requested()
        assert "https://dash.example.com" in result
        assert "Web Dashboard" in result

    @pytest.mark.asyncio
    async def test_web_url_hidden_when_empty(self):
        app = _build_app()
        app.executor.connectors = {}

        result = await app._on_health_requested()
        assert "Web Dashboard" not in result


# ── _on_gateway_status() ────────────────────────────────────────────────────


class TestOnGatewayStatus:
    @pytest.mark.asyncio
    async def test_disconnect_notification(self):
        app = _build_app()
        await app._on_gateway_status("test-acct", connected=False)

        app.db.log_audit.assert_awaited()
        app.bot.send_notification.assert_awaited_once()
        text = app.bot.send_notification.call_args[0][0]
        assert "disconnected" in text
        assert "test-acct" in text

    @pytest.mark.asyncio
    async def test_reconnect_notification(self):
        app = _build_app()
        await app._on_gateway_status("test-acct", connected=True)

        app.db.log_audit.assert_awaited()
        app.bot.send_notification.assert_awaited_once()
        text = app.bot.send_notification.call_args[0][0]
        assert "reconnected" in text

    @pytest.mark.asyncio
    async def test_no_notification_without_bot_token(self):
        app = _build_app(_make_config(bot_token=""))
        await app._on_gateway_status("test-acct", connected=False)

        app.db.log_audit.assert_awaited()
        app.bot.send_notification.assert_not_awaited()


# ── _on_order_event() ───────────────────────────────────────────────────────


class TestOnOrderEvent:
    @pytest.mark.asyncio
    async def test_submitted_event(self):
        app = _build_app()
        info = {
            "event": "submitted",
            "symbol": "AAPL",
            "local_symbol": "AAPL  280117C00085000",
            "action": "BUY",
            "qty": 5,
            "order_id": 42,
        }
        await app._on_order_event("test-acct", info)
        app.bot.send_notification.assert_awaited_once()
        text = app.bot.send_notification.call_args[0][0]
        assert "ORDER ACCEPTED" in text
        assert "BUY 5x" in text

    @pytest.mark.asyncio
    async def test_filled_event_updates_db(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        app.db.find_execution_by_order = AsyncMock(return_value={"id": 7})

        info = {
            "event": "filled",
            "symbol": "AAPL",
            "local_symbol": "AAPL  280117C00085000",
            "action": "BUY",
            "qty": 5,
            "avg_price": 12.50,
            "order_id": 42,
        }

        with patch.object(app, "_check_margin_compliance", new_callable=AsyncMock):
            await app._on_order_event("test-acct", info)

        app.db.log_audit.assert_awaited()
        app.db.update_execution_fill.assert_awaited_once_with(7, 5, 12.50, "filled")
        app.db.update_execution_allocation.assert_awaited_once()
        text = app.bot.send_notification.call_args[0][0]
        assert "ORDER FILLED" in text

    @pytest.mark.asyncio
    async def test_rejected_event(self):
        app = _build_app()
        info = {
            "event": "rejected",
            "symbol": "AAPL",
            "local_symbol": "AAPL  280117C00085000",
            "action": "BUY",
            "qty": 5,
            "order_id": 42,
            "error": "Insufficient margin",
        }
        await app._on_order_event("test-acct", info)
        app.db.log_audit.assert_awaited()
        text = app.bot.send_notification.call_args[0][0]
        assert "ORDER REJECTED" in text
        assert "Insufficient margin" in text

    @pytest.mark.asyncio
    async def test_cancelled_event(self):
        app = _build_app()
        info = {
            "event": "cancelled",
            "symbol": "AAPL",
            "local_symbol": "AAPL  280117C00085000",
            "action": "BUY",
            "qty": 5,
            "order_id": 42,
        }
        await app._on_order_event("test-acct", info)
        app.db.log_audit.assert_awaited()
        text = app.bot.send_notification.call_args[0][0]
        assert "ORDER CANCELLED" in text

    @pytest.mark.asyncio
    async def test_unknown_event_ignored(self):
        app = _build_app()
        info = {"event": "partial_fill", "symbol": "AAPL", "action": "BUY", "qty": 2, "order_id": 1}
        await app._on_order_event("test-acct", info)
        app.bot.send_notification.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_filled_no_execution_record(self):
        """Fill event with no matching execution row in DB should still notify."""
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        app.db.find_execution_by_order = AsyncMock(return_value=None)

        info = {
            "event": "filled", "symbol": "AAPL", "local_symbol": "AAPL",
            "action": "BUY", "qty": 1, "avg_price": 10.0, "order_id": 99,
        }

        with patch.object(app, "_check_margin_compliance", new_callable=AsyncMock):
            await app._on_order_event("test-acct", info)

        app.bot.send_notification.assert_awaited_once()
        app.db.update_execution_fill.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_bot_skips_notification(self):
        app = _build_app(_make_config(bot_token=""))
        info = {
            "event": "submitted", "symbol": "AAPL", "local_symbol": "AAPL",
            "action": "BUY", "qty": 1, "order_id": 1,
        }
        await app._on_order_event("test-acct", info)
        app.bot.send_notification.assert_not_awaited()


# ── _on_account_requested() ─────────────────────────────────────────────────


class TestOnAccountRequested:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_account_requested("all")
        assert result == "No IBKR accounts connected"

    @pytest.mark.asyncio
    async def test_account_not_found(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        result = await app._on_account_requested("nonexistent")
        assert "not found" in result

    @pytest.mark.asyncio
    async def test_account_detail_shown(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=80_000.0)
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_account_requested("test-acct")
        assert "Test" in result  # display_name
        assert "80,000" in result


# ── _on_deposits_requested() ────────────────────────────────────────────────


class TestOnDepositsRequested:
    @pytest.mark.asyncio
    async def test_no_transactions(self):
        app = _build_app()
        result = await app._on_deposits_requested()
        assert "Deposit / Withdrawal History" in result
        assert "Test" in result

    @pytest.mark.asyncio
    async def test_with_transactions(self):
        app = _build_app()
        app.db.get_cash_transactions = AsyncMock(return_value=[
            {"amount": 5000.0, "report_date": "20260101"},
            {"amount": -1000.0, "report_date": "20260215"},
        ])
        result = await app._on_deposits_requested()
        assert "5,000" in result
        assert "-1,000" in result


# ── _on_cancel_order() / _on_cancel_all_orders() ────────────────────────────


class TestOrderCancellation:
    @pytest.mark.asyncio
    async def test_cancel_specific_order(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_cancel_order("test-acct", 42)
        conn.cancel_order.assert_awaited_once_with(42)
        assert "cancel requested" in result

    @pytest.mark.asyncio
    async def test_cancel_order_not_found(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.cancel_order = AsyncMock(return_value=False)
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_cancel_order("test-acct", 999)
        assert "not found" in result

    @pytest.mark.asyncio
    async def test_cancel_order_disconnected(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_cancel_order("test-acct", 42)
        assert "not connected" in result

    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_cancel_all_orders()
        assert result == "No IBKR accounts connected"

    @pytest.mark.asyncio
    async def test_cancel_all_orders(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_cancel_all_orders()
        conn.cancel_all_orders.assert_awaited_once()
        assert "cancelled 3 orders" in result


# ── _on_orders_requested() ──────────────────────────────────────────────────


class TestOnOrdersRequested:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        text, orders = await app._on_orders_requested()
        assert "No IBKR accounts connected" in text
        assert orders == []

    @pytest.mark.asyncio
    async def test_no_open_orders(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.get_open_orders = MagicMock(return_value=[])
        app.executor.connectors = {"test-acct": conn}

        text, orders = await app._on_orders_requested()
        assert "No open orders" in text
        assert orders == []

    @pytest.mark.asyncio
    async def test_with_open_orders(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.get_open_orders = MagicMock(return_value=[{
            "order_id": 42,
            "action": "BUY",
            "qty": 5,
            "local_symbol": "AAPL  280117C00085000",
            "limit_price": 12.50,
            "status": "Submitted",
            "filled": 0,
        }])
        app.executor.connectors = {"test-acct": conn}

        text, orders = await app._on_orders_requested()
        assert "#42" in text
        assert "BUY" in text
        assert len(orders) == 1
        assert orders[0]["order_id"] == 42


# ── _on_trades_requested() ──────────────────────────────────────────────────


class TestOnTradesRequested:
    @pytest.mark.asyncio
    async def test_no_trades_today(self):
        app = _build_app()
        app.db.get_executions_since = AsyncMock(return_value=[])

        result = await app._on_trades_requested()
        assert "No trades found" in result
        assert "Today" in result

    @pytest.mark.asyncio
    async def test_no_trades_week(self):
        app = _build_app()
        app.db.get_executions_since = AsyncMock(return_value=[])

        result = await app._on_trades_requested(period="week")
        assert "No trades found" in result
        assert "Past 7 Days" in result

    @pytest.mark.asyncio
    async def test_with_trades(self):
        app = _build_app()
        app.db.get_executions_since = AsyncMock(return_value=[{
            "account_name": "test-acct",
            "signal_action": "BUY",
            "ticker": "AAPL",
            "filled_qty": 5,
            "avg_price": 12.50,
            "status": "filled",
            "created_at": "2026-04-20T10:00:00",
            "error": None,
        }])

        result = await app._on_trades_requested()
        assert "AAPL" in result
        assert "Test" in result  # display_name


# ── _on_list_positions() ────────────────────────────────────────────────────


class TestOnListPositions:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_list_positions()
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_held_tickers(self):
        app = _build_app()
        pos = MagicMock()
        pos.contract.symbol = "AAPL"
        pos.contract.lastTradeDateOrContractMonth = "20280117"
        pos.contract.strike = 85.0
        pos.contract.right = "C"
        pos.position = 10
        conn = _make_connector_mock(positions=[pos])
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_list_positions()
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"
        assert result[0]["total_qty"] == 10


# ── _parse_order_args() ─────────────────────────────────────────────────────


class TestParseOrderArgs:
    def test_basic_pct_with_price(self):
        app = _build_app()
        result = app._parse_order_args("IREN 5% 12.75")
        assert result["ticker"] == "IREN"
        assert result["pct"] == 5.0
        assert result["limit_price"] == 12.75

    def test_market_order(self):
        app = _build_app()
        result = app._parse_order_args("IREN all mkt")
        assert result["pct"] == "all"
        assert result["limit_price"] == "MKT"

    def test_half_sell(self):
        app = _build_app()
        result = app._parse_order_args("IREN half 1.60")
        assert result["pct"] == "half"
        assert result["limit_price"] == 1.60

    def test_specific_contract(self):
        app = _build_app()
        result = app._parse_order_args("CIFR 5% Jan28 27C 8.50")
        assert result["ticker"] == "CIFR"
        assert result["pct"] == 5.0
        assert result["expiry"] == "20280117"
        assert result["strike"] == 27.0
        assert result["right"] == "C"
        assert result["limit_price"] == 8.50

    def test_no_ticker_raises(self):
        app = _build_app()
        with pytest.raises(ValueError, match="No ticker"):
            app._parse_order_args("")

    def test_no_price_raises(self):
        app = _build_app()
        with pytest.raises(ValueError, match="limit price"):
            app._parse_order_args("IREN 5%")

    def test_put_option(self):
        app = _build_app()
        result = app._parse_order_args("AAPL 3% Jan28 150P 5.00")
        assert result["right"] == "P"
        assert result["strike"] == 150.0


# ── _resolve_accounts() ─────────────────────────────────────────────────────


class TestResolveAccounts:
    def test_all_filter(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        result = app._resolve_accounts("all")
        assert len(result) == 1
        assert result[0].name == "test-acct"

    def test_specific_name(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        result = app._resolve_accounts("test-acct")
        assert len(result) == 1

    def test_no_match(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        result = app._resolve_accounts("nonexistent")
        assert result == []

    def test_disconnected_excluded(self):
        app = _build_app()
        # Connector not in executor.connectors means disconnected
        app.executor.connectors = {}
        result = app._resolve_accounts("all")
        assert result == []


# ── _on_pause_requested() / _on_resume_requested() ──────────────────────────


class TestPauseResume:
    @pytest.mark.asyncio
    async def test_pause_already_paused(self):
        app = _build_app()
        app._gateway_paused = True
        result = await app._on_pause_requested(10)
        assert "already paused" in result

    @pytest.mark.asyncio
    async def test_resume_already_running(self):
        app = _build_app()
        app._gateway_paused = False
        result = await app._on_resume_requested()
        assert "already running" in result

    @pytest.mark.asyncio
    async def test_pause_docker_failure(self):
        app = _build_app()
        with patch("src.app.docker") as mock_docker:
            mock_docker.DockerClient.side_effect = RuntimeError("no socket")
            result = await app._on_pause_requested(10)
        assert "Failed to connect to Docker" in result
        assert app._gateway_paused is False


# ── _on_price_requested() ───────────────────────────────────────────────────


class TestOnPriceRequested:
    @pytest.mark.asyncio
    async def test_empty_ticker_raises(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        with pytest.raises(ValueError, match="Specify a ticker"):
            await app._on_price_requested("")

    @pytest.mark.asyncio
    async def test_no_connection_raises(self):
        app = _build_app()
        # All connectors disconnected
        conn = _make_connector_mock()
        conn.is_connected = False
        app.executor.connectors = {"test-acct": conn}
        with pytest.raises(ValueError, match="No IBKR connection"):
            await app._on_price_requested("AAPL")


# ── _sync_positions() ───────────────────────────────────────────────────────


class TestSyncPositions:
    @pytest.mark.asyncio
    async def test_sync_empty_portfolio(self):
        app = _build_app()
        conn = _make_connector_mock(portfolio=[])
        conn.managed_accounts = MagicMock(return_value=[])
        app.executor.connectors = {"test-acct": conn}

        await app._sync_positions()

        app.db.upsert_account_summary.assert_awaited_once()
        app.db.delete_stale_positions.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sync_with_portfolio_items(self):
        app = _build_app()
        item = MagicMock()
        item.contract.localSymbol = "AAPL  280117C00085000"
        item.contract.symbol = "AAPL"
        item.marketValue = 5000.0
        item.unrealizedPNL = 200.0
        item.position = 10
        item.averageCost = 48.0
        item.marketPrice = 50.0

        conn = _make_connector_mock(portfolio=[item])
        app.executor.connectors = {"test-acct": conn}

        await app._sync_positions()

        app.db.upsert_position.assert_awaited_once()
        call_kwargs = app.db.upsert_position.call_args.kwargs
        assert call_kwargs["ticker"] == "AAPL  280117C00085000"
        assert call_kwargs["quantity"] == 10

    @pytest.mark.asyncio
    async def test_sync_exception_handled(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.get_portfolio = AsyncMock(side_effect=Exception("network error"))
        app.executor.connectors = {"test-acct": conn}

        # Should not raise
        await app._sync_positions()


# ── run() ────────────────────────────────────────────────────────────────────


class TestRun:
    @pytest.mark.asyncio
    async def test_nothing_configured_shuts_down(self):
        """No bot, no accounts, no webhook -> logs error and shuts down."""
        app = _build_app(_make_config(bot_token="", accounts=[], webhook_secret=""))

        with patch.object(app, "shutdown", new_callable=AsyncMock) as mock_shutdown:
            await app.run()

        mock_shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_init_db_called(self):
        """run() always calls db.init first."""
        app = _build_app(_make_config(bot_token="", accounts=[], webhook_secret=""))

        with patch.object(app, "shutdown", new_callable=AsyncMock):
            await app.run()

        app.db.init.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_accounts_trigger_connect_and_sync(self):
        """When accounts are configured, connect_all and sync run."""
        app = _build_app()
        # Make bot.start and periodic_sync return immediately
        app.bot.start = AsyncMock()

        with (
            patch.object(app, "_periodic_sync", new_callable=AsyncMock),
            patch.object(app, "_sync_flex_deposits", new_callable=AsyncMock),
            patch.object(app, "_sync_positions", new_callable=AsyncMock),
            patch.object(app, "shutdown", new_callable=AsyncMock),
        ):
            await app.run()

        app.executor.connect_all.assert_awaited_once()


# ── _on_info_requested() ────────────────────────────────────────────────────


class TestOnInfoRequested:
    @pytest.mark.asyncio
    async def test_no_connectors(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_info_requested("AAPL")
        assert "No IBKR accounts connected" in result

    @pytest.mark.asyncio
    async def test_no_position_found(self):
        app = _build_app()
        conn = _make_connector_mock(positions=[])
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_info_requested("AAPL")
        assert "no position" in result


# ── _sync_flex_deposits() ───────────────────────────────────────────────────


class TestSyncFlexDeposits:
    @pytest.mark.asyncio
    async def test_skips_accounts_without_flex(self):
        app = _build_app()
        # Default account has no flex_token/flex_query_id
        await app._sync_flex_deposits()
        # No fetch attempt
        app.db.upsert_cash_transaction.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flex_sync_updates_deposits(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="flex-acct", gateway_host="gw", gateway_port=4003,
                display_name="Flex", net_deposits=5000.0,
                flex_token="tok123", flex_query_id=99999,
            ),
        ])
        app = _build_app(cfg)
        app.db.get_net_deposits = AsyncMock(return_value=2000.0)

        with patch.object(
            type(app), "_fetch_flex_transactions",
            staticmethod(lambda token, qid: [
                {"date": "20260101", "amount": 2000.0, "currency": "EUR", "description": "dep"},
            ]),
        ):
            await app._sync_flex_deposits()

        app.db.upsert_cash_transaction.assert_awaited_once()
        app.db.update_account_deposits.assert_awaited_once()
        # net_deposits = baseline(5000) + db_net(2000) = 7000
        assert cfg.accounts[0].net_deposits == 7000.0

    @pytest.mark.asyncio
    async def test_flex_sync_handles_exception(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="flex-acct", gateway_host="gw", gateway_port=4003,
                flex_token="tok", flex_query_id=1,
            ),
        ])
        app = _build_app(cfg)

        with patch(
            "asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=Exception("network error"),
        ):
            # Should not raise
            await app._sync_flex_deposits()


# ── Helpers for new tests ──────────────────────────────────────────────────


def _make_portfolio_item(
    symbol="AAPL", position=10, unrealized_pnl=500.0, market_value=15000.0,
    avg_cost=12.5, market_price=15.0, con_id=12345, strike=0.0, right="",
    exp="", local_symbol=None,
):
    """Build a mock portfolio item for _on_value_requested / _sync tests."""
    item = MagicMock()
    item.contract.symbol = symbol
    item.contract.conId = con_id
    item.contract.localSymbol = local_symbol or (
        f"{symbol} 250117C00085000" if strike else symbol
    )
    item.contract.strike = strike
    item.contract.right = right
    item.contract.lastTradeDateOrContractMonth = exp
    item.position = position
    item.unrealizedPNL = unrealized_pnl
    item.marketValue = market_value
    item.averageCost = avg_cost
    item.marketPrice = market_price
    return item


def _make_position_mock(symbol="AAPL", position=10, avg_cost=1250.0,
                        con_id=12345, strike=85.0, right="C",
                        exp="20280117", local_symbol=None):
    """Build a mock position object (from get_positions)."""
    pos = MagicMock()
    pos.contract.symbol = symbol
    pos.contract.conId = con_id
    pos.contract.strike = strike
    pos.contract.right = right
    pos.contract.lastTradeDateOrContractMonth = exp
    pos.contract.localSymbol = local_symbol or f"{symbol}  {exp}{'C' if right == 'C' else 'P'}00{int(strike)}000"
    pos.position = position
    pos.avgCost = avg_cost
    return pos


# ── _on_value_requested() ──────────────────────────────────────────────────


class TestOnValueRequested:
    @pytest.mark.asyncio
    async def test_no_connectors_returns_warning(self):
        app = _build_app()
        app.executor.connectors = {}
        result = await app._on_value_requested()
        assert "No IBKR accounts connected" in result

    @pytest.mark.asyncio
    async def test_disconnected_connector_shown(self):
        app = _build_app()
        # One connected account so we enter the loop, but a second configured
        # account has no connector — shown as disconnected
        cfg2 = AccountConfig(
            name="acct-b", gateway_host="gw-b", gateway_port=4004,
            display_name="B",
        )
        app.config.accounts.append(cfg2)
        conn = _make_connector_mock(nlv=50_000.0, portfolio=[])
        app.executor.connectors = {"test-acct": conn}
        result = await app._on_value_requested()
        assert "disconnected" in result

    @pytest.mark.asyncio
    async def test_no_positions_shows_no_positions(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=50_000.0, portfolio=[])
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_value_requested()
        assert "No positions" in result
        assert "Portfolio" in result

    @pytest.mark.asyncio
    async def test_with_positions_shows_details(self):
        app = _build_app()
        item = _make_portfolio_item(
            symbol="AAPL", position=10, unrealized_pnl=500.0,
            market_value=15000.0, avg_cost=12.5, market_price=15.0,
            con_id=12345,
        )
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[item])
        conn.get_positions_daily_pnl = AsyncMock(return_value={12345: 200.0})
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_value_requested()
        assert "AAPL" in result
        assert "Portfolio" in result
        assert "Positions" in result

    @pytest.mark.asyncio
    async def test_mode_day_shows_today(self):
        app = _build_app()
        item = _make_portfolio_item(
            symbol="AAPL", position=10, unrealized_pnl=500.0,
            market_value=15000.0, avg_cost=12.5, market_price=15.0,
            con_id=12345,
        )
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[item])
        conn.get_positions_daily_pnl = AsyncMock(return_value={12345: 200.0})
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_value_requested(mode="day")
        assert "Today" in result

    @pytest.mark.asyncio
    async def test_mode_total_shows_pnl(self):
        app = _build_app()
        item = _make_portfolio_item(
            symbol="AAPL", position=10, unrealized_pnl=500.0,
            market_value=15000.0, avg_cost=12.5, market_price=15.0,
            con_id=12345,
        )
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[item])
        conn.get_positions_daily_pnl = AsyncMock(return_value={12345: 200.0})
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_value_requested(mode="total")
        assert "P&L" in result

    @pytest.mark.asyncio
    async def test_cash_shown_when_positions_dont_fill_100(self):
        app = _build_app()
        # Small position relative to NLV => large cash weight
        item = _make_portfolio_item(
            symbol="AAPL", position=1, unrealized_pnl=50.0,
            market_value=1000.0, avg_cost=9.5, market_price=10.0,
            con_id=12345,
        )
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[item])
        conn.get_positions_daily_pnl = AsyncMock(return_value={})
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_value_requested()
        assert "Cash" in result


# ── _parse_order_args() — additional cases ─────────────────────────────────


class TestParseOrderArgsExtended:
    def test_quarter(self):
        app = _build_app()
        result = app._parse_order_args("IREN quarter 5.00")
        assert result["pct"] == "quarter"
        assert result["limit_price"] == 5.0

    def test_third(self):
        app = _build_app()
        result = app._parse_order_args("IREN third 5.00")
        assert result["pct"] == "third"
        assert result["limit_price"] == 5.0

    def test_full_date_format(self):
        app = _build_app()
        result = app._parse_order_args("CIFR 5% 20280121 27C 8.50")
        assert result["expiry"] == "20280121"
        assert result["strike"] == 27.0
        assert result["right"] == "C"

    def test_invalid_percentage_raises(self):
        app = _build_app()
        with pytest.raises(ValueError, match="Invalid percentage"):
            app._parse_order_args("IREN abc 5.00")

    def test_all_keyword(self):
        app = _build_app()
        result = app._parse_order_args("IREN all mkt")
        assert result["pct"] == "all"
        assert result["limit_price"] == "MKT"

    def test_numeric_pct_without_sign(self):
        app = _build_app()
        result = app._parse_order_args("IREN 10 12.00")
        assert result["pct"] == 10.0
        assert result["limit_price"] == 12.0


# ── _on_buy_preview() ──────────────────────────────────────────────────────


class TestOnBuyPreview:
    @pytest.mark.asyncio
    async def test_no_connectors_raises(self):
        app = _build_app()
        app.executor.connectors = {}
        with pytest.raises(ValueError, match="No IBKR accounts connected"):
            await app._on_buy_preview("IREN 5% 12.75")

    @pytest.mark.asyncio
    async def test_named_pct_raises(self):
        """Using 'all' or 'half' for buy should raise."""
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        with pytest.raises(ValueError, match="percentage"):
            await app._on_buy_preview("IREN all 12.75")

    @pytest.mark.asyncio
    async def test_exceeds_max_single_order_pct(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}
        with pytest.raises(ValueError, match="Safety limit"):
            await app._on_buy_preview("IREN 25% 12.75")

    @pytest.mark.asyncio
    async def test_basic_buy_preview(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 100_000, "USD": 110_000})
        conn.get_available_funds = AsyncMock(return_value=50_000.0)

        contract_mock = MagicMock()
        contract_mock.conId = 999
        contract_mock.localSymbol = "IREN  280117C00015000"
        contract_mock.lastTradeDateOrContractMonth = "20280117"
        contract_mock.strike = 15.0
        contract_mock.right = "C"
        contract_mock.symbol = "IREN"

        conn.find_leaps_contract = AsyncMock(return_value=contract_mock)
        conn.get_option_price = AsyncMock(return_value=5.0)
        app.executor.connectors = {"test-acct": conn}

        text, order_details = await app._on_buy_preview("IREN 5% 12.75")
        assert "BUY Order Preview" in text
        assert "IREN" in text
        assert order_details["action"] == "BUY"
        assert order_details["ticker"] == "IREN"
        assert order_details["limit_price"] == 12.75
        assert "test-acct" in order_details["allocations"]

    @pytest.mark.asyncio
    async def test_position_limit_reduces_qty(self):
        """When existing position + new order > MAX_TOTAL_POSITION_PCT, qty reduced."""
        app = _build_app()
        # Existing position already at 30% of NLV
        existing = _make_portfolio_item(
            symbol="IREN", market_value=33_000.0, con_id=999,
        )
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[existing])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 100_000, "USD": 110_000})
        conn.get_available_funds = AsyncMock(return_value=50_000.0)

        contract_mock = MagicMock()
        contract_mock.conId = 999
        contract_mock.localSymbol = "IREN  280117C00015000"
        contract_mock.lastTradeDateOrContractMonth = "20280117"
        contract_mock.strike = 15.0
        contract_mock.right = "C"
        contract_mock.symbol = "IREN"

        conn.find_leaps_contract = AsyncMock(return_value=contract_mock)
        conn.get_option_price = AsyncMock(return_value=5.0)
        app.executor.connectors = {"test-acct": conn}

        text, order_details = await app._on_buy_preview("IREN 10% 12.75")
        # Should have a safety warning about position limit
        assert "Safety Warnings" in text or order_details["allocations"]["test-acct"] < 22

    @pytest.mark.asyncio
    async def test_insufficient_funds_skips(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 100_000, "USD": 110_000})
        conn.get_available_funds = AsyncMock(return_value=0.0)  # No funds

        contract_mock = MagicMock()
        contract_mock.conId = 999
        contract_mock.localSymbol = "IREN  280117C00015000"
        contract_mock.lastTradeDateOrContractMonth = "20280117"
        contract_mock.strike = 15.0
        contract_mock.right = "C"
        contract_mock.symbol = "IREN"

        conn.find_leaps_contract = AsyncMock(return_value=contract_mock)
        conn.get_option_price = AsyncMock(return_value=5.0)
        app.executor.connectors = {"test-acct": conn}

        with pytest.raises(ValueError, match="No viable allocations"):
            await app._on_buy_preview("IREN 5% 12.75")

    @pytest.mark.asyncio
    async def test_specific_contract_preview(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 100_000, "USD": 110_000})
        conn.get_available_funds = AsyncMock(return_value=50_000.0)

        contract_mock = MagicMock()
        contract_mock.conId = 888
        contract_mock.localSymbol = "CIFR  280117C00027000"
        contract_mock.lastTradeDateOrContractMonth = "20280117"
        contract_mock.strike = 27.0
        contract_mock.right = "C"
        contract_mock.symbol = "CIFR"

        conn.qualify_contracts = AsyncMock(return_value=[contract_mock])
        conn.get_option_price = AsyncMock(return_value=8.50)
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Option") as mock_option:
            mock_option.return_value = MagicMock()
            text, details = await app._on_buy_preview("CIFR 5% Jan28 27C 8.50")

        assert "CIFR" in text
        assert details["strike"] == 27.0


# ── _on_sell_preview() ──────────────────────────────────────────────────────


class TestOnSellPreview:
    @pytest.mark.asyncio
    async def test_no_connectors_raises(self):
        app = _build_app()
        app.executor.connectors = {}
        with pytest.raises(ValueError, match="No IBKR accounts connected"):
            await app._on_sell_preview("IREN all 1.60")

    @pytest.mark.asyncio
    async def test_no_position_found_raises(self):
        app = _build_app()
        conn = _make_connector_mock(positions=[])
        app.executor.connectors = {"test-acct": conn}
        with pytest.raises(ValueError, match="No open position found"):
            await app._on_sell_preview("IREN all 1.60")

    @pytest.mark.asyncio
    async def test_sell_all(self):
        pos = _make_position_mock(symbol="IREN", position=10, strike=15.0, right="C")
        app = _build_app()
        conn = _make_connector_mock(positions=[pos])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 1.50, "ask": 1.70, "mid": 1.60, "last": 1.55,
            "close": 1.50, "spread": 0.20, "spread_pct": 12.5,
        })
        app.executor.connectors = {"test-acct": conn}

        text, details = await app._on_sell_preview("IREN all 1.60")
        assert "SELL Order Preview" in text
        assert details["action"] == "SELL"
        assert details["allocations"]["test-acct"] == 10

    @pytest.mark.asyncio
    async def test_sell_half(self):
        pos = _make_position_mock(symbol="IREN", position=10, strike=15.0, right="C")
        app = _build_app()
        conn = _make_connector_mock(positions=[pos])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 1.50, "ask": 1.70, "mid": 1.60, "last": 1.55,
            "close": 1.50, "spread": 0.20, "spread_pct": 2.0,
        })
        app.executor.connectors = {"test-acct": conn}

        text, details = await app._on_sell_preview("IREN half 1.60")
        assert details["allocations"]["test-acct"] == 5

    @pytest.mark.asyncio
    async def test_wide_spread_warning(self):
        pos = _make_position_mock(symbol="IREN", position=10, strike=15.0, right="C")
        app = _build_app()
        conn = _make_connector_mock(positions=[pos])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 1.00, "ask": 2.00, "mid": 1.50, "last": 1.40,
            "close": 1.30, "spread": 1.00, "spread_pct": 66.7,
        })
        app.executor.connectors = {"test-acct": conn}

        text, details = await app._on_sell_preview("IREN all 1.50")
        assert "Wide spread" in text


# ── _on_buy_execute() ──────────────────────────────────────────────────────


class TestOnBuyExecute:
    @pytest.mark.asyncio
    async def test_successful_execution(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 100_000, "USD": 110_000})
        conn.get_available_funds = AsyncMock(return_value=50_000.0)

        contract_mock = MagicMock()
        contract_mock.conId = 999
        contract_mock.localSymbol = "IREN  280117C00015000"

        conn.qualify_contracts = AsyncMock(return_value=[contract_mock])
        conn.place_order = AsyncMock(return_value=42)
        app.executor.connectors = {"test-acct": conn}

        order_details = {
            "action": "BUY",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 5.0,
            "limit_price": 12.75,
            "allocations": {"test-acct": 3},
            "target_pct": 5.0,
        }

        with patch("ib_async.Option") as mock_option, \
             patch("ib_async.LimitOrder") as mock_limit_order:
            mock_option.return_value = MagicMock()
            mock_limit_order.return_value = MagicMock()
            result = await app._on_buy_execute(order_details)

        assert "#42" in result
        assert "BUY 3x" in result

    @pytest.mark.asyncio
    async def test_disconnected_account(self):
        app = _build_app()
        app.executor.connectors = {}  # no connector

        order_details = {
            "action": "BUY",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 5.0,
            "limit_price": "MKT",
            "allocations": {"test-acct": 3},
            "target_pct": 5.0,
        }

        result = await app._on_buy_execute(order_details)
        assert "disconnected" in result

    @pytest.mark.asyncio
    async def test_safety_revalidation_blocks(self):
        """Live re-check blocks when order now exceeds single-order cap."""
        app = _build_app()
        conn = _make_connector_mock(nlv=100_000.0, portfolio=[])
        # NLV shrunk so 3 contracts at $50/ea ($15000) > 20% of $60k NLV
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 54_000, "USD": 60_000})
        conn.get_available_funds = AsyncMock(return_value=50_000.0)
        app.executor.connectors = {"test-acct": conn}

        order_details = {
            "action": "BUY",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 50.0,  # $5000 per contract, 3x = $15000 = 25% of $60k
            "limit_price": 50.0,
            "allocations": {"test-acct": 3},
            "target_pct": 5.0,
        }

        with patch("ib_async.Option"), patch("ib_async.LimitOrder"):
            result = await app._on_buy_execute(order_details)

        assert "BLOCKED" in result


# ── _on_sell_execute() ──────────────────────────────────────────────────────


class TestOnSellExecute:
    @pytest.mark.asyncio
    async def test_successful_sell(self):
        pos = _make_position_mock(symbol="IREN", position=10, strike=15.0, right="C")
        app = _build_app()
        conn = _make_connector_mock(positions=[pos])
        conn.place_order = AsyncMock(return_value=55)
        app.executor.connectors = {"test-acct": conn}

        order_details = {
            "action": "SELL",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 1.60,
            "limit_price": 1.60,
            "allocations": {"test-acct": 5},
        }

        with patch("ib_async.LimitOrder") as mock_limit_order:
            mock_limit_order.return_value = MagicMock()
            result = await app._on_sell_execute(order_details)

        assert "#55" in result
        assert "SELL 5x" in result

    @pytest.mark.asyncio
    async def test_sell_disconnected_account(self):
        app = _build_app()
        app.executor.connectors = {}

        order_details = {
            "action": "SELL",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 1.60,
            "limit_price": "MKT",
            "allocations": {"test-acct": 5},
        }

        result = await app._on_sell_execute(order_details)
        assert "disconnected" in result

    @pytest.mark.asyncio
    async def test_sell_position_gone(self):
        """Position disappeared between preview and execute."""
        app = _build_app()
        conn = _make_connector_mock(positions=[])  # No matching position
        app.executor.connectors = {"test-acct": conn}

        order_details = {
            "action": "SELL",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 1.60,
            "limit_price": 1.60,
            "allocations": {"test-acct": 5},
        }

        result = await app._on_sell_execute(order_details)
        assert "position gone" in result

    @pytest.mark.asyncio
    async def test_sell_market_order(self):
        pos = _make_position_mock(symbol="IREN", position=10, strike=15.0, right="C")
        app = _build_app()
        conn = _make_connector_mock(positions=[pos])
        conn.place_order = AsyncMock(return_value=66)
        app.executor.connectors = {"test-acct": conn}

        order_details = {
            "action": "SELL",
            "ticker": "IREN",
            "expiry": "20280117",
            "strike": 15.0,
            "right": "C",
            "option_price": 1.60,
            "limit_price": "MKT",
            "allocations": {"test-acct": 10},
        }

        with patch("ib_async.MarketOrder") as mock_market_order:
            mock_market_order.return_value = MagicMock()
            result = await app._on_sell_execute(order_details)

        assert "#66" in result
        assert "MKT" in result


# ── _check_margin_compliance() ─────────────────────────────────────────────


class TestCheckMarginCompliance:
    @pytest.mark.asyncio
    async def test_margin_off_skips(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="test-acct", gateway_host="gw", gateway_port=4003,
                display_name="Test", margin_mode="off", max_margin_usd=0,
            ),
        ])
        app = _build_app(cfg)
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        await app._check_margin_compliance()
        conn.get_margin_used.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_soft_under_cap_skips(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="test-acct", gateway_host="gw", gateway_port=4003,
                display_name="Test", margin_mode="soft", max_margin_usd=10_000,
            ),
        ])
        app = _build_app(cfg)
        conn = _make_connector_mock()
        conn.get_margin_used = AsyncMock(return_value=5_000.0)  # Under cap
        app.executor.connectors = {"test-acct": conn}

        await app._check_margin_compliance()
        app.bot.send_notification.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_soft_over_cap_sends_alert(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="test-acct", gateway_host="gw", gateway_port=4003,
                display_name="Test", margin_mode="soft", max_margin_usd=10_000,
            ),
        ])
        app = _build_app(cfg)
        conn = _make_connector_mock()
        conn.get_margin_used = AsyncMock(return_value=15_000.0)  # Over cap
        app.executor.connectors = {"test-acct": conn}

        await app._check_margin_compliance()
        app.bot.send_notification.assert_awaited_once()
        text = app.bot.send_notification.call_args[0][0]
        assert "MARGIN WARNING" in text
        assert "No action taken" in text

    @pytest.mark.asyncio
    async def test_hard_over_cap_auto_sells(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="test-acct", gateway_host="gw", gateway_port=4003,
                display_name="Test", margin_mode="hard", max_margin_usd=10_000,
            ),
        ])
        app = _build_app(cfg)

        item = _make_portfolio_item(
            symbol="AAPL", position=5, market_value=5000.0, market_price=10.0,
        )
        conn = _make_connector_mock()
        conn.get_margin_used = AsyncMock(return_value=15_000.0)
        conn.get_portfolio = AsyncMock(return_value=[item])
        conn.place_order = AsyncMock(return_value=77)

        # Simulate trade fill on second poll
        trade_mock = MagicMock()
        trade_mock.order.orderId = 77
        trade_mock.orderStatus.status = "Filled"
        trade_mock.orderStatus.avgFillPrice = 10.0
        conn.get_trades = MagicMock(return_value=[trade_mock])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.MarketOrder") as mock_market_order:
            mock_market_order.return_value = MagicMock()
            await app._check_margin_compliance()

        conn.place_order.assert_awaited_once()
        # Should have sent at least 2 notifications (breach + result)
        assert app.bot.send_notification.await_count >= 2

    @pytest.mark.asyncio
    async def test_hard_no_positions_to_sell(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="test-acct", gateway_host="gw", gateway_port=4003,
                display_name="Test", margin_mode="hard", max_margin_usd=10_000,
            ),
        ])
        app = _build_app(cfg)
        conn = _make_connector_mock()
        conn.get_margin_used = AsyncMock(return_value=15_000.0)
        conn.get_portfolio = AsyncMock(return_value=[])  # No positions
        app.executor.connectors = {"test-acct": conn}

        # Should not raise; just logs error
        await app._check_margin_compliance()
        conn.place_order = AsyncMock()
        conn.place_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_specific_account_filter(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="test-acct", gateway_host="gw", gateway_port=4003,
                display_name="Test", margin_mode="soft", max_margin_usd=10_000,
            ),
        ])
        app = _build_app(cfg)
        conn = _make_connector_mock()
        conn.get_margin_used = AsyncMock(return_value=15_000.0)
        app.executor.connectors = {"test-acct": conn}

        await app._check_margin_compliance(account_name="test-acct")
        app.bot.send_notification.assert_awaited_once()


# ── _sync_positions() — extended coverage ──────────────────────────────────


class TestSyncPositionsExtended:
    @pytest.mark.asyncio
    async def test_nlv_currency_conversion_eur_only(self):
        """EUR-only account computes USD from exchange rate."""
        app = _build_app()
        item = _make_portfolio_item(symbol="AAPL")
        conn = _make_connector_mock(portfolio=[item])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 50_000.0})
        conn.get_exchange_rate = AsyncMock(return_value=0.88)
        app.executor.connectors = {"test-acct": conn}

        await app._sync_positions()

        call_kwargs = app.db.upsert_account_summary.call_args.kwargs
        # nlv_usd should be computed: 50000 / 0.88
        assert call_kwargs["nlv_usd"] > 0
        assert call_kwargs["nlv_eur"] == 50_000.0

    @pytest.mark.asyncio
    async def test_nlv_currency_conversion_usd_only(self):
        """USD-only account computes EUR from exchange rate."""
        app = _build_app()
        item = _make_portfolio_item(symbol="AAPL")
        conn = _make_connector_mock(portfolio=[item])
        conn.get_nlv_by_currency = AsyncMock(return_value={"USD": 110_000.0})
        conn.get_exchange_rate = AsyncMock(return_value=0.88)
        app.executor.connectors = {"test-acct": conn}

        await app._sync_positions()

        call_kwargs = app.db.upsert_account_summary.call_args.kwargs
        # nlv_eur should be computed: 110000 * 0.88
        assert call_kwargs["nlv_eur"] > 0
        assert call_kwargs["nlv_usd"] == 110_000.0

    @pytest.mark.asyncio
    async def test_resubscribe_when_portfolio_empty_but_connected(self):
        """Connected but empty portfolio triggers re-subscribe."""
        app = _build_app()
        conn = _make_connector_mock(portfolio=[])
        conn.is_connected = True
        conn.managed_accounts = MagicMock(return_value=["U1234567"])
        # Second call returns data after re-subscribe
        conn.get_portfolio = AsyncMock(side_effect=[[], []])
        app.executor.connectors = {"test-acct": conn}

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await app._sync_positions()

        conn.req_account_updates.assert_called_once_with(True, "U1234567")


# ── _on_info_requested() — extended coverage ───────────────────────────────


class TestOnInfoRequestedExtended:
    @pytest.mark.asyncio
    async def test_has_position_with_live_data(self):
        pos = _make_position_mock(
            symbol="IREN", position=5, avg_cost=850.0,
            strike=15.0, right="C", exp="20280117",
        )
        app = _build_app()
        conn = _make_connector_mock(positions=[pos], portfolio=[])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 8.00, "ask": 9.00, "mid": 8.50, "spread": 1.00,
            "spread_pct": 11.8, "delta": 0.65, "gamma": 0.03,
            "theta": -0.05, "iv": 0.45,
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_info_requested("IREN")
        assert "IREN" in result
        assert "Bid" in result
        assert "Ask" in result
        assert "Spread" in result
        assert "WIDE" in result  # spread_pct > 5

    @pytest.mark.asyncio
    async def test_fallback_to_cached_data(self):
        pos = _make_position_mock(
            symbol="IREN", position=5, avg_cost=850.0,
            strike=15.0, right="C", exp="20280117", con_id=999,
        )
        cached_item = _make_portfolio_item(
            symbol="IREN", con_id=999, market_price=8.50,
            market_value=4250.0, unrealized_pnl=200.0,
        )
        app = _build_app()
        conn = _make_connector_mock(positions=[pos], portfolio=[cached_item])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 0, "ask": 0, "mid": 0, "spread": 0,
            "spread_pct": 0, "delta": None, "gamma": None,
            "theta": None, "iv": None,
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_info_requested("IREN")
        assert "cached" in result

    @pytest.mark.asyncio
    async def test_greeks_displayed_with_iv(self):
        pos = _make_position_mock(symbol="IREN", position=5, avg_cost=850.0)
        app = _build_app()
        conn = _make_connector_mock(positions=[pos], portfolio=[])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 8.00, "ask": 9.00, "mid": 8.50, "spread": 1.00,
            "spread_pct": 3.0, "delta": 0.65, "gamma": 0.03,
            "theta": -0.05, "iv": 0.45,
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_info_requested("IREN")
        assert "IV=" in result
        assert "\u0394=" in result  # Delta symbol

    @pytest.mark.asyncio
    async def test_greeks_without_iv(self):
        pos = _make_position_mock(symbol="IREN", position=5, avg_cost=850.0)
        app = _build_app()
        conn = _make_connector_mock(positions=[pos], portfolio=[])
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 8.00, "ask": 9.00, "mid": 8.50, "spread": 1.00,
            "spread_pct": 3.0, "delta": 0.65, "gamma": 0.03,
            "theta": -0.05, "iv": None,
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_info_requested("IREN")
        assert "\u0394=" in result
        assert "IV=" not in result


# ── _on_price_requested() — extended coverage ──────────────────────────────


class TestOnPriceRequestedExtended:
    @pytest.mark.asyncio
    async def test_stock_price_with_option_positions(self):
        item = _make_portfolio_item(
            symbol="IREN", position=5, con_id=999, strike=15.0, right="C",
            exp="20280117", market_price=8.50, market_value=4250.0,
        )
        app = _build_app()
        conn = _make_connector_mock(portfolio=[item])
        conn.get_stock_prices_batch = AsyncMock(return_value={
            "IREN": {"price": 14.50, "low": 14.00, "high": 15.00},
        })
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 8.00, "ask": 9.00, "mid": 8.50,
            "spread": 1.00, "spread_pct": 11.8,
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_price_requested("IREN")
        assert "Quote" in result
        assert "Stock" in result
        assert "14.50" in result
        assert "Bid" in result

    @pytest.mark.asyncio
    async def test_no_positions_for_ticker(self):
        app = _build_app()
        conn = _make_connector_mock(portfolio=[])
        conn.get_stock_prices_batch = AsyncMock(return_value={
            "AAPL": {"price": 180.0, "low": 178.0, "high": 182.0},
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_price_requested("AAPL")
        assert "No open positions" in result

    @pytest.mark.asyncio
    async def test_stock_price_error_handled(self):
        app = _build_app()
        conn = _make_connector_mock(portfolio=[])
        conn.get_stock_prices_batch = AsyncMock(side_effect=Exception("timeout"))
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_price_requested("AAPL")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_cached_fallback_when_no_live_data(self):
        item = _make_portfolio_item(
            symbol="IREN", position=5, con_id=999, strike=15.0, right="C",
            exp="20280117", market_price=8.50, market_value=4250.0,
        )
        app = _build_app()
        conn = _make_connector_mock(portfolio=[item])
        conn.get_stock_prices_batch = AsyncMock(return_value={})
        # Live snapshot returns zeros
        conn.get_option_detail = AsyncMock(return_value={
            "bid": 0, "ask": 0, "mid": 0,
            "spread": 0, "spread_pct": 0,
        })
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_price_requested("IREN")
        assert "cached" in result


# ── _on_option_expiries() ────────────────────────────────────────────────────


class TestOnOptionExpiries:
    @pytest.mark.asyncio
    async def test_no_connectors_raises(self):
        app = _build_app()
        app.executor.connectors = {}
        with pytest.raises(ValueError, match="No IBKR connection"):
            await app._on_option_expiries("AAPL")

    @pytest.mark.asyncio
    async def test_unknown_ticker_raises(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.qualify_contracts = AsyncMock(return_value=[])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            with pytest.raises(ValueError, match="Unknown ticker"):
                await app._on_option_expiries("ZZZZ")

    @pytest.mark.asyncio
    async def test_no_chains_raises(self):
        app = _build_app()
        conn = _make_connector_mock()
        qualified = MagicMock()
        qualified.symbol = "AAPL"
        qualified.secType = "STK"
        qualified.conId = 12345
        conn.qualify_contracts = AsyncMock(return_value=[qualified])
        conn.get_option_chain_params = AsyncMock(return_value=[])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            with pytest.raises(ValueError, match="No option chains"):
                await app._on_option_expiries("AAPL")

    @pytest.mark.asyncio
    async def test_returns_long_dated_expiries(self):
        app = _build_app()
        conn = _make_connector_mock()
        qualified = MagicMock()
        qualified.symbol = "AAPL"
        qualified.secType = "STK"
        qualified.conId = 12345
        conn.qualify_contracts = AsyncMock(return_value=[qualified])

        chain = MagicMock()
        chain.exchange = "SMART"
        chain.expirations = ["20270115", "20280121"]
        chain.strikes = [100, 150, 200]
        conn.get_option_chain_params = AsyncMock(return_value=[chain])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            result = await app._on_option_expiries("AAPL")

        assert len(result) >= 1
        assert "exp" in result[0]
        assert "display" in result[0]
        assert "dte" in result[0]

    @pytest.mark.asyncio
    async def test_no_long_dated_expiries_raises(self):
        """All expiries are too soon (< 180 DTE)."""
        app = _build_app()
        conn = _make_connector_mock()
        qualified = MagicMock()
        qualified.symbol = "AAPL"
        qualified.secType = "STK"
        qualified.conId = 12345
        conn.qualify_contracts = AsyncMock(return_value=[qualified])

        chain = MagicMock()
        chain.exchange = "SMART"
        # Use a date that's definitely in the past
        chain.expirations = ["20240115"]
        chain.strikes = [100]
        conn.get_option_chain_params = AsyncMock(return_value=[chain])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            with pytest.raises(ValueError, match="No long-dated expiries"):
                await app._on_option_expiries("AAPL")

    @pytest.mark.asyncio
    async def test_fallback_to_first_chain_when_no_smart(self):
        """When no SMART exchange chain, falls back to the first one."""
        app = _build_app()
        conn = _make_connector_mock()
        qualified = MagicMock()
        qualified.symbol = "AAPL"
        qualified.secType = "STK"
        qualified.conId = 12345
        conn.qualify_contracts = AsyncMock(return_value=[qualified])

        chain = MagicMock()
        chain.exchange = "CBOE"
        chain.expirations = ["20280121"]
        chain.strikes = [100]
        conn.get_option_chain_params = AsyncMock(return_value=[chain])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            result = await app._on_option_expiries("AAPL")

        assert len(result) >= 1


# ── _on_option_strikes() ────────────────────────────────────────────────────


class TestOnOptionStrikes:
    @pytest.mark.asyncio
    async def test_no_connectors_raises(self):
        app = _build_app()
        app.executor.connectors = {}
        with pytest.raises(ValueError, match="No IBKR connection"):
            await app._on_option_strikes("AAPL", "20280121")

    @pytest.mark.asyncio
    async def test_unknown_ticker_raises(self):
        app = _build_app()
        conn = _make_connector_mock()
        conn.qualify_contracts = AsyncMock(return_value=[])
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            with pytest.raises(ValueError, match="Unknown ticker"):
                await app._on_option_strikes("ZZZZ", "20280121")

    @pytest.mark.asyncio
    async def test_call_strikes_returned(self):
        app = _build_app()
        conn = _make_connector_mock()
        qualified = MagicMock()
        qualified.symbol = "AAPL"
        qualified.secType = "STK"
        qualified.conId = 12345
        conn.qualify_contracts = AsyncMock(return_value=[qualified])

        chain = MagicMock()
        chain.exchange = "SMART"
        chain.expirations = ["20280121"]
        chain.strikes = [100, 120, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 250]
        conn.get_option_chain_params = AsyncMock(return_value=[chain])
        conn.get_current_price = AsyncMock(return_value=180.0)
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            result = await app._on_option_strikes("AAPL", "20280121", "C")

        assert result["ticker"] == "AAPL"
        assert result["expiry"] == "20280121"
        assert result["right"] == "C"
        assert result["current_price"] == 180.0
        assert len(result["strikes"]) > 0
        # Verify ITM/OTM labels
        labels = {s["label"] for s in result["strikes"]}
        assert "ITM" in labels or "OTM" in labels

    @pytest.mark.asyncio
    async def test_put_strikes_returned(self):
        app = _build_app()
        conn = _make_connector_mock()
        qualified = MagicMock()
        qualified.symbol = "AAPL"
        qualified.secType = "STK"
        qualified.conId = 12345
        conn.qualify_contracts = AsyncMock(return_value=[qualified])

        chain = MagicMock()
        chain.exchange = "SMART"
        chain.expirations = ["20280121"]
        chain.strikes = [100, 120, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 250]
        conn.get_option_chain_params = AsyncMock(return_value=[chain])
        conn.get_current_price = AsyncMock(return_value=180.0)
        app.executor.connectors = {"test-acct": conn}

        with patch("ib_async.Stock"):
            result = await app._on_option_strikes("AAPL", "20280121", "P")

        assert result["right"] == "P"
        assert len(result["strikes"]) > 0
        # For puts: ITM = strike > price
        itm_strikes = [s for s in result["strikes"] if s["label"] == "ITM"]
        for s in itm_strikes:
            assert s["strike"] > 180.0


# ── _fetch_flex_transactions() ───────────────────────────────────────────────


class TestFetchFlexTransactions:
    def test_successful_fetch(self):
        from src.app import App

        # Step 1 response: success with reference code
        resp1_xml = (
            b'<FlexStatementResponse><Status>Success</Status>'
            b'<ReferenceCode>REF123</ReferenceCode>'
            b'</FlexStatementResponse>'
        )
        # Step 2 response: statement with transactions
        dep = b'<CashTransaction type="Deposits/Withdrawals"'
        resp2_xml = (
            b'<FlexQueryResponse><FlexStatements><FlexStatement>'
            b'<CashTransactions>'
            + dep + b' reportDate="20260101" amount="5000"'
            b' currency="EUR" description="Deposit"/>'
            + dep + b' reportDate="20260201" amount="-1000"'
            b' currency="EUR" description="Withdrawal"/>'
            b'<CashTransaction type="Interest"'
            b' reportDate="20260301" amount="10"'
            b' currency="EUR" description="Interest"/>'
            b'</CashTransactions>'
            b'</FlexStatement></FlexStatements></FlexQueryResponse>'
        )

        mock_resp1 = MagicMock()
        mock_resp1.read.return_value = resp1_xml
        mock_resp2 = MagicMock()
        mock_resp2.read.return_value = resp2_xml

        with patch("urllib.request.urlopen", side_effect=[mock_resp1, mock_resp2]), \
             patch("time.sleep"):
            result = App._fetch_flex_transactions("tok123", 99999)

        assert result is not None
        assert len(result) == 2  # Interest excluded
        assert result[0]["amount"] == 5000.0
        assert result[1]["amount"] == -1000.0

    def test_request_fails(self):
        from src.app import App
        resp_xml = (
            b'<FlexStatementResponse><Status>Fail</Status>'
            b'<ErrorMessage>Invalid token</ErrorMessage>'
            b'</FlexStatementResponse>'
        )
        mock_resp = MagicMock()
        mock_resp.read.return_value = resp_xml

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = App._fetch_flex_transactions("bad-token", 1)

        assert result is None

    def test_no_reference_code(self):
        from src.app import App
        resp_xml = (
            b'<FlexStatementResponse><Status>Success</Status>'
            b'<ReferenceCode></ReferenceCode>'
            b'</FlexStatementResponse>'
        )
        mock_resp = MagicMock()
        mock_resp.read.return_value = resp_xml

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = App._fetch_flex_transactions("tok", 1)

        assert result is None

    def test_statement_generation_in_progress_retries(self):
        from src.app import App
        resp1_xml = (
            b'<FlexStatementResponse><Status>Success</Status>'
            b'<ReferenceCode>REF1</ReferenceCode>'
            b'</FlexStatementResponse>'
        )
        mock_resp1 = MagicMock()
        mock_resp1.read.return_value = resp1_xml

        # First retrieve: "in progress"; second: actual data
        in_progress_resp = MagicMock()
        in_progress_resp.read.return_value = b'Statement generation in progress'
        final_resp = MagicMock()
        final_resp.read.return_value = (
            b'<FlexQueryResponse><FlexStatements><FlexStatement>'
            b'<CashTransactions></CashTransactions>'
            b'</FlexStatement></FlexStatements></FlexQueryResponse>'
        )

        with patch("urllib.request.urlopen", side_effect=[mock_resp1, in_progress_resp, final_resp]), \
             patch("time.sleep"):
            result = App._fetch_flex_transactions("tok", 1)

        assert result is not None
        assert result == []

    def test_all_retries_in_progress(self):
        from src.app import App
        resp1_xml = (
            b'<FlexStatementResponse><Status>Success</Status>'
            b'<ReferenceCode>REF1</ReferenceCode>'
            b'</FlexStatementResponse>'
        )
        mock_resp1 = MagicMock()
        mock_resp1.read.return_value = resp1_xml

        in_progress = MagicMock()
        in_progress.read.return_value = b'Statement generation in progress'

        with patch("urllib.request.urlopen", side_effect=[mock_resp1] + [in_progress] * 5), \
             patch("time.sleep"):
            result = App._fetch_flex_transactions("tok", 1)

        assert result is None

    def test_deduplication(self):
        from src.app import App
        resp1_xml = (
            b'<FlexStatementResponse><Status>Success</Status>'
            b'<ReferenceCode>REF1</ReferenceCode>'
            b'</FlexStatementResponse>'
        )
        mock_resp1 = MagicMock()
        mock_resp1.read.return_value = resp1_xml

        # Two identical transactions should be deduped
        dep = b'<CashTransaction type="Deposits/Withdrawals"'
        resp2_xml = (
            b'<FlexQueryResponse><FlexStatements><FlexStatement>'
            b'<CashTransactions>'
            + dep + b' reportDate="20260101" amount="5000"'
            b' currency="EUR" description="Deposit"/>'
            + dep + b' reportDate="20260101" amount="5000"'
            b' currency="EUR" description="Deposit"/>'
            b'</CashTransactions>'
            b'</FlexStatement></FlexStatements></FlexQueryResponse>'
        )
        mock_resp2 = MagicMock()
        mock_resp2.read.return_value = resp2_xml

        with patch("urllib.request.urlopen", side_effect=[mock_resp1, mock_resp2]), \
             patch("time.sleep"):
            result = App._fetch_flex_transactions("tok", 1)

        assert len(result) == 1


# ── _on_pause_requested() / _on_resume_requested() — extended ───────────────


class TestPauseResumeExtended:
    @pytest.mark.asyncio
    async def test_pause_stops_containers(self):
        app = _build_app()
        with patch("src.app.docker") as mock_docker:
            mock_client = MagicMock()
            mock_docker.DockerClient.return_value = mock_client
            mock_container = MagicMock()
            mock_container.stop = MagicMock()
            mock_client.containers.get.return_value = mock_container

            result = await app._on_pause_requested(10)

        assert "paused" in result
        assert app._gateway_paused is True
        assert "gw-test" in result

    @pytest.mark.asyncio
    async def test_pause_container_stop_failure(self):
        """Individual container stop failure is logged but doesn't crash."""
        app = _build_app()
        with patch("src.app.docker") as mock_docker:
            mock_client = MagicMock()
            mock_docker.DockerClient.return_value = mock_client
            mock_client.containers.get.side_effect = Exception("not found")

            result = await app._on_pause_requested(10)

        # Still reports as paused (containers may be gone)
        assert app._gateway_paused is True
        assert "paused" in result

    @pytest.mark.asyncio
    async def test_resume_starts_containers(self):
        app = _build_app()
        app._gateway_paused = True
        app.executor.connect_all = AsyncMock()

        with patch("src.app.docker") as mock_docker, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_client = MagicMock()
            mock_docker.DockerClient.return_value = mock_client
            mock_container = MagicMock()
            mock_container.start = MagicMock()
            mock_client.containers.get.return_value = mock_container

            result = await app._on_resume_requested()

        assert "resumed" in result
        assert app._gateway_paused is False

    @pytest.mark.asyncio
    async def test_resume_docker_failure(self):
        app = _build_app()
        app._gateway_paused = True

        with patch("src.app.docker") as mock_docker:
            mock_docker.DockerClient.side_effect = RuntimeError("no socket")
            result = await app._on_resume_requested()

        assert "Failed to connect to Docker" in result

    @pytest.mark.asyncio
    async def test_resume_reconnect_failure(self):
        app = _build_app()
        app._gateway_paused = True
        app.executor.connect_all = AsyncMock(side_effect=Exception("timeout"))

        with patch("src.app.docker") as mock_docker, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_client = MagicMock()
            mock_docker.DockerClient.return_value = mock_client
            mock_container = MagicMock()
            mock_container.start = MagicMock()
            mock_client.containers.get.return_value = mock_container

            result = await app._on_resume_requested()

        assert "reconnect failed" in result
        assert app._gateway_paused is False


# ── _auto_resume() ──────────────────────────────────────────────────────────


class TestAutoResume:
    @pytest.mark.asyncio
    async def test_auto_resume_calls_resume(self):
        app = _build_app()
        app._gateway_paused = True
        app.executor.connect_all = AsyncMock()

        with patch("src.app.docker") as mock_docker, \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_client = MagicMock()
            mock_docker.DockerClient.return_value = mock_client
            mock_container = MagicMock()
            mock_container.start = MagicMock()
            mock_client.containers.get.return_value = mock_container

            await app._auto_resume(1)

        app.bot.send_notification.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auto_resume_cancelled(self):
        app = _build_app()

        with patch("asyncio.sleep", new_callable=AsyncMock,
                   side_effect=asyncio.CancelledError):
            # Should not raise
            await app._auto_resume(10)

        app.bot.send_notification.assert_not_awaited()


# ── _periodic_sync() ────────────────────────────────────────────────────────


class TestPeriodicSync:
    @pytest.mark.asyncio
    async def test_calls_sync_and_subscribe(self):
        app = _build_app()
        conn = _make_connector_mock()
        app.executor.connectors = {"test-acct": conn}

        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()

        mock_sync = AsyncMock()
        mock_margin = AsyncMock()
        with patch("asyncio.sleep", side_effect=fake_sleep), \
             patch.object(app, "_sync_positions", mock_sync), \
             patch.object(app, "_check_margin_compliance", mock_margin):
            with pytest.raises(asyncio.CancelledError):
                await app._periodic_sync()

            mock_sync.assert_awaited_once()
            conn.subscribe_pnl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_when_paused(self):
        app = _build_app()
        app._gateway_paused = True

        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()

        mock_sync = AsyncMock()
        with patch("asyncio.sleep", side_effect=fake_sleep), \
             patch.object(app, "_sync_positions", mock_sync):
            with pytest.raises(asyncio.CancelledError):
                await app._periodic_sync()

            mock_sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exception_handling(self):
        app = _build_app()
        app._gateway_paused = False

        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()

        with patch("asyncio.sleep", side_effect=fake_sleep), \
             patch.object(app, "_sync_positions", new_callable=AsyncMock,
                         side_effect=Exception("sync failed")):
            with pytest.raises(asyncio.CancelledError):
                await app._periodic_sync()

        # Should not crash, continues to next iteration

    @pytest.mark.asyncio
    async def test_flex_sync_every_60_cycles(self):
        """Every 60 iterations (1 hour), sync flex deposits."""
        app = _build_app()
        app._gateway_paused = False

        call_count = 0

        async def fake_sleep(seconds):
            nonlocal call_count
            call_count += 1
            # Run 61 iterations to trigger flex sync at cycle 60
            if call_count > 61:
                raise asyncio.CancelledError()

        mock_flex = AsyncMock()
        with patch("asyncio.sleep", side_effect=fake_sleep), \
             patch.object(app, "_sync_positions", AsyncMock()), \
             patch.object(app, "_check_margin_compliance", AsyncMock()), \
             patch.object(app, "_sync_flex_deposits", mock_flex):
            with pytest.raises(asyncio.CancelledError):
                await app._periodic_sync()

            mock_flex.assert_awaited_once()


# ── _on_deposits_requested() — extended coverage ────────────────────────────


class TestOnDepositsRequestedExtended:
    @pytest.mark.asyncio
    async def test_with_transactions_and_withdrawals(self):
        app = _build_app()
        app.db.get_cash_transactions = AsyncMock(return_value=[
            {"amount": 5000.0, "report_date": "20260101"},
            {"amount": -1000.0, "report_date": "20260215"},
        ])
        result = await app._on_deposits_requested()
        assert "5,000" in result
        assert "-1,000" in result
        assert "Tracked deposits" in result
        assert "Tracked withdrawals" in result
        assert "Total net deposited" in result

    @pytest.mark.asyncio
    async def test_account_filter_specific(self):
        app = _build_app()
        app.db.get_cash_transactions = AsyncMock(return_value=[])
        result = await app._on_deposits_requested("test-acct")
        assert "Test" in result

    @pytest.mark.asyncio
    async def test_account_filter_no_match(self):
        app = _build_app()
        result = await app._on_deposits_requested("nonexistent")
        assert "Deposit / Withdrawal History" in result
        assert "Test" not in result

    @pytest.mark.asyncio
    async def test_baseline_shown(self):
        cfg = _make_config()
        cfg.accounts[0].net_deposits = 10_000
        app = _build_app(cfg)
        app._deposit_baselines = {"test-acct": 10_000}
        app.db.get_cash_transactions = AsyncMock(return_value=[])

        result = await app._on_deposits_requested()
        assert "Pre-Flex baseline" in result
        assert "10,000" in result

    @pytest.mark.asyncio
    async def test_combined_summary_multiple_accounts(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="acct-a", gateway_host="gw-a", gateway_port=4003,
                display_name="A", net_deposits=5000.0,
            ),
            AccountConfig(
                name="acct-b", gateway_host="gw-b", gateway_port=4004,
                display_name="B", net_deposits=3000.0,
            ),
        ])
        app = _build_app(cfg)
        app._deposit_baselines = {"acct-a": 5000, "acct-b": 3000}
        app.db.get_cash_transactions = AsyncMock(return_value=[
            {"amount": 2000.0, "report_date": "20260101"},
        ])

        result = await app._on_deposits_requested("all")
        assert "Combined" in result

    @pytest.mark.asyncio
    async def test_date_formatting_8_digit(self):
        app = _build_app()
        app.db.get_cash_transactions = AsyncMock(return_value=[
            {"amount": 500.0, "report_date": "20260315"},
        ])
        result = await app._on_deposits_requested()
        assert "2026-03-15" in result


# ── _sync_flex_deposits() — extended coverage ────────────────────────────────


class TestSyncFlexDepositsExtended:
    @pytest.mark.asyncio
    async def test_flex_returns_none_still_updates_deposits(self):
        """When fetch returns None, deposits should still update from DB."""
        cfg = _make_config(accounts=[
            AccountConfig(
                name="flex-acct", gateway_host="gw", gateway_port=4003,
                display_name="Flex", net_deposits=5000.0,
                flex_token="tok123", flex_query_id=99999,
            ),
        ])
        app = _build_app(cfg)
        app._deposit_baselines = {"flex-acct": 5000}
        app.db.get_net_deposits = AsyncMock(return_value=1000.0)

        with patch("asyncio.to_thread", new_callable=AsyncMock, return_value=None):
            await app._sync_flex_deposits()

        # Still updates deposits from DB even when fetch returns None
        app.db.get_net_deposits.assert_awaited_once()
        assert cfg.accounts[0].net_deposits == 6000.0

    @pytest.mark.asyncio
    async def test_flex_transactions_upserted(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="flex-acct", gateway_host="gw", gateway_port=4003,
                display_name="Flex", net_deposits=0.0,
                flex_token="tok", flex_query_id=1,
            ),
        ])
        app = _build_app(cfg)
        app._deposit_baselines = {"flex-acct": 0}
        app.db.get_net_deposits = AsyncMock(return_value=7000.0)

        txns = [
            {"date": "20260101", "amount": 5000.0, "currency": "EUR", "description": "dep"},
            {"date": "20260201", "amount": 2000.0, "currency": "EUR", "description": "dep2"},
        ]

        with patch("asyncio.to_thread", new_callable=AsyncMock, return_value=txns):
            await app._sync_flex_deposits()

        assert app.db.upsert_cash_transaction.await_count == 2
        app.db.update_account_deposits.assert_awaited_once()


# ── _on_account_requested() — extended coverage ─────────────────────────────


class TestOnAccountRequestedExtended:
    @pytest.mark.asyncio
    async def test_account_with_positions_shows_pnl(self):
        app = _build_app()
        item = _make_portfolio_item(
            symbol="AAPL", position=10, unrealized_pnl=500.0,
            market_value=15000.0, avg_cost=12.5, market_price=15.0,
        )
        conn = _make_connector_mock(nlv=80_000.0, portfolio=[item])
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 80_000.0})
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_account_requested("test-acct")
        assert "Test" in result
        assert "80,000" in result
        assert "AAPL" in result
        assert "Return" in result

    @pytest.mark.asyncio
    async def test_account_only_connected_shown(self):
        """Only connected accounts appear when filter is 'all'."""
        cfg = _make_config(accounts=[
            AccountConfig(
                name="acct-a", gateway_host="gw-a", gateway_port=4003,
                display_name="A",
            ),
            AccountConfig(
                name="acct-b", gateway_host="gw-b", gateway_port=4004,
                display_name="B",
            ),
        ])
        app = _build_app(cfg)
        conn = _make_connector_mock(nlv=50_000.0)
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 50_000.0})
        # Only acct-a connected
        app.executor.connectors = {"acct-a": conn}

        result = await app._on_account_requested("all")
        assert "A" in result
        # acct-b not in connectors, so not in _resolve_accounts result
        assert "NLV" in result

    @pytest.mark.asyncio
    async def test_account_by_display_name(self):
        app = _build_app()
        conn = _make_connector_mock(nlv=60_000.0)
        conn.get_nlv_by_currency = AsyncMock(return_value={"EUR": 60_000.0})
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_account_requested("test")
        assert "Test" in result
        assert "60,000" in result


# ── _on_list_positions() — extended coverage ─────────────────────────────────


class TestOnListPositionsExtended:
    @pytest.mark.asyncio
    async def test_skips_zero_positions(self):
        app = _build_app()
        pos_zero = MagicMock()
        pos_zero.contract.symbol = "DEAD"
        pos_zero.position = 0
        pos_good = MagicMock()
        pos_good.contract.symbol = "AAPL"
        pos_good.contract.lastTradeDateOrContractMonth = "20280117"
        pos_good.contract.strike = 85.0
        pos_good.contract.right = "C"
        pos_good.position = 5
        conn = _make_connector_mock(positions=[pos_zero, pos_good])
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_list_positions()
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_aggregates_across_accounts(self):
        cfg = _make_config(accounts=[
            AccountConfig(
                name="acct-a", gateway_host="gw-a", gateway_port=4003,
                display_name="A",
            ),
            AccountConfig(
                name="acct-b", gateway_host="gw-b", gateway_port=4004,
                display_name="B",
            ),
        ])
        app = _build_app(cfg)

        pos_a = MagicMock()
        pos_a.contract.symbol = "AAPL"
        pos_a.contract.lastTradeDateOrContractMonth = "20280117"
        pos_a.contract.strike = 85.0
        pos_a.contract.right = "C"
        pos_a.position = 5

        pos_b = MagicMock()
        pos_b.contract.symbol = "AAPL"
        pos_b.contract.lastTradeDateOrContractMonth = "20280117"
        pos_b.contract.strike = 85.0
        pos_b.contract.right = "C"
        pos_b.position = 3

        conn_a = _make_connector_mock(positions=[pos_a])
        conn_b = _make_connector_mock(positions=[pos_b])
        app.executor.connectors = {"acct-a": conn_a, "acct-b": conn_b}

        result = await app._on_list_positions()
        assert len(result) == 1
        assert result[0]["total_qty"] == 8

    @pytest.mark.asyncio
    async def test_stock_position_no_strike(self):
        app = _build_app()
        pos = MagicMock()
        pos.contract.symbol = "AAPL"
        pos.contract.lastTradeDateOrContractMonth = ""
        pos.contract.strike = 0
        pos.contract.right = ""
        pos.position = 100
        conn = _make_connector_mock(positions=[pos])
        app.executor.connectors = {"test-acct": conn}

        result = await app._on_list_positions()
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"
        assert result[0]["desc"] == "AAPL"
