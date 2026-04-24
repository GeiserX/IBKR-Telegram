"""Tests for SQLite database — signals, executions, positions, audit, accounts, cash."""

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from src.db import Database


@pytest.fixture
async def db():
    """Create an in-memory database, initialize schema, yield, then close."""
    database = Database(":memory:")
    await database.init()
    yield database
    await database.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _insert_signal(db: Database, **overrides) -> int:
    defaults = {
        "message_id": 1,
        "ticker": "AAPL",
        "action": "BUY",
        "target_weight_pct": 5.0,
        "amount_description": "5%",
        "related_ticker": None,
        "raw_text": "Buy AAPL 5%",
        "source": "text",
    }
    defaults.update(overrides)
    return await db.save_signal(**defaults)


async def _insert_execution(db: Database, signal_id: int, **overrides) -> int:
    defaults = {
        "signal_id": signal_id,
        "account_name": "U12345",
        "order_id": 100,
        "filled_qty": 10,
        "avg_price": 150.0,
        "target_pct": 5.0,
        "actual_pct": 4.8,
        "status": "filled",
    }
    defaults.update(overrides)
    return await db.save_execution(**defaults)


# ===========================================================================
# init / schema
# ===========================================================================


class TestInit:
    async def test_init_creates_tables(self, db: Database):
        """Tables exist after init()."""
        cursor = await db._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        rows = await cursor.fetchall()
        names = sorted(r[0] for r in rows)
        for expected in [
            "account_summary", "audit_log", "cash_transactions",
            "executions", "positions", "signals",
        ]:
            assert expected in names

    async def test_wal_mode_set(self, db: Database):
        """init() issues PRAGMA journal_mode=WAL; in-memory DBs report 'memory'."""
        cursor = await db._db.execute("PRAGMA journal_mode")
        row = await cursor.fetchone()
        # In-memory databases cannot use WAL; SQLite silently keeps 'memory'.
        assert row[0] == "memory"

    async def test_init_idempotent(self, db: Database):
        """Calling init() a second time must not raise."""
        await db.init()
        # Still functional
        sid = await _insert_signal(db)
        assert sid >= 1

    async def test_migration_non_duplicate_error_logs_warning(self, caplog):
        """Non-duplicate-column ALTER errors are logged as warnings."""
        database = Database(":memory:")
        await database.init()

        # Patch execute on the live connection to fail on ALTER TABLE
        conn = database._db
        original_execute = conn.execute

        async def raise_on_alter(sql, *a, **kw):
            if isinstance(sql, str) and sql.startswith("ALTER TABLE"):
                raise Exception("table locked or something")
            return await original_execute(sql, *a, **kw)

        conn.execute = raise_on_alter

        # Make aiosqlite.connect return a fake awaitable that yields the same conn
        mock_conn = AsyncMock(return_value=conn)

        with (
            patch("src.db.aiosqlite.connect", mock_conn),
            caplog.at_level(logging.WARNING, logger="src.db"),
        ):
            await database.init()

        assert any("Migration warning" in r.message for r in caplog.records)
        conn.execute = original_execute
        await database.close()

    async def test_migration_columns_exist(self, db: Database):
        """Migration ALTER TABLEs add extra columns to account_summary."""
        cursor = await db._db.execute("PRAGMA table_info(account_summary)")
        cols = {r[1] for r in await cursor.fetchall()}
        for col in ("base_currency", "nlv_eur", "nlv_usd",
                     "exchange_rate", "net_deposits", "display_name"):
            assert col in cols


# ===========================================================================
# save_signal / get_recent_signals
# ===========================================================================


class TestSignals:
    async def test_save_signal_returns_id(self, db: Database):
        sid = await _insert_signal(db)
        assert isinstance(sid, int)
        assert sid >= 1

    async def test_save_signal_persists(self, db: Database):
        sid = await _insert_signal(db, ticker="MSFT", action="SELL")
        rows = await db.get_recent_signals(limit=10)
        assert len(rows) == 1
        assert rows[0]["ticker"] == "MSFT"
        assert rows[0]["action"] == "SELL"
        assert rows[0]["id"] == sid

    async def test_get_recent_signals_respects_limit(self, db: Database):
        for i in range(5):
            await _insert_signal(db, message_id=i)
        rows = await db.get_recent_signals(limit=3)
        assert len(rows) == 3

    async def test_get_recent_signals_ordered_desc(self, db: Database):
        s1 = await _insert_signal(db, message_id=1)
        s2 = await _insert_signal(db, message_id=2)
        rows = await db.get_recent_signals(limit=10)
        assert rows[0]["id"] == s2
        assert rows[1]["id"] == s1

    async def test_save_signal_with_none_weight(self, db: Database):
        await _insert_signal(db, target_weight_pct=None)
        rows = await db.get_recent_signals()
        assert rows[0]["target_weight_pct"] is None

    async def test_save_signal_with_related_ticker(self, db: Database):
        await _insert_signal(db, related_ticker="SPY")
        rows = await db.get_recent_signals()
        assert rows[0]["related_ticker"] == "SPY"


# ===========================================================================
# update_signal_status
# ===========================================================================


class TestUpdateSignalStatus:
    async def test_update_status(self, db: Database):
        sid = await _insert_signal(db)
        await db.update_signal_status(sid, "executed")
        rows = await db.get_recent_signals()
        assert rows[0]["status"] == "executed"

    async def test_update_status_multiple_times(self, db: Database):
        sid = await _insert_signal(db)
        await db.update_signal_status(sid, "confirmed")
        await db.update_signal_status(sid, "executed")
        rows = await db.get_recent_signals()
        assert rows[0]["status"] == "executed"


# ===========================================================================
# save_execution / find_execution_by_order
# ===========================================================================


class TestExecutions:
    async def test_save_execution_returns_id(self, db: Database):
        sid = await _insert_signal(db)
        eid = await _insert_execution(db, sid)
        assert isinstance(eid, int)
        assert eid >= 1

    async def test_save_execution_deviation_calculated(self, db: Database):
        sid = await _insert_signal(db)
        await db.save_execution(
            signal_id=sid, account_name="U1", order_id=1,
            filled_qty=10, avg_price=100.0,
            target_pct=5.0, actual_pct=4.5, status="filled",
        )
        row = await db.find_execution_by_order("U1", 1)
        assert row is not None
        assert abs(row["deviation_pct"] - 0.5) < 1e-9

    async def test_save_execution_zero_actual_pct(self, db: Database):
        """When actual_pct is 0 (falsy), deviation should be 0."""
        sid = await _insert_signal(db)
        await db.save_execution(
            signal_id=sid, account_name="U1", order_id=2,
            filled_qty=0, avg_price=0.0,
            target_pct=5.0, actual_pct=0, status="error",
        )
        row = await db.find_execution_by_order("U1", 2)
        assert row["deviation_pct"] == 0

    async def test_save_execution_with_error(self, db: Database):
        sid = await _insert_signal(db)
        await db.save_execution(
            signal_id=sid, account_name="U1", order_id=3,
            filled_qty=0, avg_price=0, target_pct=5.0,
            actual_pct=0, status="error", error="Insufficient funds",
        )
        row = await db.find_execution_by_order("U1", 3)
        assert row["error"] == "Insufficient funds"
        assert row["status"] == "error"

    async def test_find_execution_by_order_not_found(self, db: Database):
        result = await db.find_execution_by_order("NOEXIST", 999)
        assert result is None

    async def test_find_execution_by_order_returns_latest(self, db: Database):
        sid = await _insert_signal(db)
        await db.save_execution(
            signal_id=sid, account_name="U1", order_id=5,
            filled_qty=5, avg_price=100.0,
            target_pct=3.0, actual_pct=2.9, status="partial",
        )
        await db.save_execution(
            signal_id=sid, account_name="U1", order_id=5,
            filled_qty=10, avg_price=101.0,
            target_pct=3.0, actual_pct=3.1, status="filled",
        )
        row = await db.find_execution_by_order("U1", 5)
        assert row["filled_qty"] == 10
        assert row["status"] == "filled"


# ===========================================================================
# update_execution_fill
# ===========================================================================


class TestUpdateExecutionFill:
    async def test_updates_fill_fields(self, db: Database):
        sid = await _insert_signal(db)
        eid = await _insert_execution(db, sid, filled_qty=0, avg_price=0.0, status="pending")
        await db.update_execution_fill(eid, filled_qty=15, avg_price=155.5, status="filled")
        row = await db.find_execution_by_order("U12345", 100)
        assert row["filled_qty"] == 15
        assert abs(row["avg_price"] - 155.5) < 1e-9
        assert row["status"] == "filled"
        assert row["filled_at"] is not None


# ===========================================================================
# update_execution_allocation
# ===========================================================================


class TestUpdateExecutionAllocation:
    async def test_updates_allocation_and_deviation(self, db: Database):
        sid = await _insert_signal(db)
        eid = await db.save_execution(
            signal_id=sid, account_name="U1", order_id=10,
            filled_qty=10, avg_price=100.0,
            target_pct=5.0, actual_pct=4.0, status="filled",
        )
        await db.update_execution_allocation(eid, actual_pct=5.2)
        row = await db.find_execution_by_order("U1", 10)
        assert abs(row["actual_allocation_pct"] - 5.2) < 1e-9
        assert abs(row["deviation_pct"] - 0.2) < 1e-9


# ===========================================================================
# get_last_sync_time
# ===========================================================================


class TestGetLastSyncTime:
    async def test_returns_none_when_empty(self, db: Database):
        result = await db.get_last_sync_time()
        assert result is None

    async def test_returns_latest_timestamp(self, db: Database):
        await db.upsert_account_summary("A1", nlv=10000)
        await db.upsert_account_summary("A2", nlv=20000)
        result = await db.get_last_sync_time()
        assert result is not None
        from datetime import datetime
        datetime.fromisoformat(result)  # validates ISO-8601 format


# ===========================================================================
# update_account_deposits
# ===========================================================================


class TestUpdateAccountDeposits:
    async def test_updates_net_deposits(self, db: Database):
        await db.upsert_account_summary("U1", nlv=50000, net_deposits=0)
        await db.update_account_deposits("U1", 10000.0)
        cursor = await db._db.execute(
            "SELECT net_deposits FROM account_summary WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row[0] == 10000.0

    async def test_noop_when_account_missing(self, db: Database):
        """Updating a nonexistent account is a no-op (0 rows affected)."""
        await db.update_account_deposits("NOPE", 5000.0)
        cursor = await db._db.execute("SELECT COUNT(*) FROM account_summary")
        row = await cursor.fetchone()
        assert row[0] == 0


# ===========================================================================
# upsert_position / get_positions / delete_stale_positions
# ===========================================================================


class TestPositions:
    async def test_insert_new_position(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0, 155.0, 10.0, 500.0)
        rows = await db.get_positions()
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"
        assert rows[0]["quantity"] == 100

    async def test_upsert_updates_existing(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U1", "AAPL", 200, 155.0, 160.0, 12.0, 1000.0)
        rows = await db.get_positions()
        assert len(rows) == 1
        assert rows[0]["quantity"] == 200
        assert rows[0]["avg_cost"] == 155.0

    async def test_upsert_different_accounts_separate(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U2", "AAPL", 50, 148.0)
        rows = await db.get_positions()
        assert len(rows) == 2

    async def test_get_positions_filter_by_account(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U2", "MSFT", 50, 300.0)
        rows = await db.get_positions(account_name="U1")
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"

    async def test_get_positions_no_filter_returns_all(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U2", "MSFT", 50, 300.0)
        rows = await db.get_positions()
        assert len(rows) == 2

    async def test_get_positions_ordered_by_ticker(self, db: Database):
        await db.upsert_position("U1", "MSFT", 10, 300.0)
        await db.upsert_position("U1", "AAPL", 20, 150.0)
        rows = await db.get_positions(account_name="U1")
        assert rows[0]["ticker"] == "AAPL"
        assert rows[1]["ticker"] == "MSFT"

    async def test_upsert_position_with_none_optional_fields(self, db: Database):
        await db.upsert_position("U1", "TSLA", 5, 200.0)
        rows = await db.get_positions()
        assert rows[0]["current_price"] is None
        assert rows[0]["weight_pct"] is None
        assert rows[0]["pnl"] is None


class TestDeleteStalePositions:
    async def test_delete_stale_with_active_tickers(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U1", "MSFT", 50, 300.0)
        await db.upsert_position("U1", "TSLA", 10, 200.0)
        deleted = await db.delete_stale_positions("U1", {"AAPL", "MSFT"})
        assert deleted == 1
        rows = await db.get_positions(account_name="U1")
        tickers = {r["ticker"] for r in rows}
        assert tickers == {"AAPL", "MSFT"}

    async def test_delete_stale_empty_active_deletes_all(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U1", "MSFT", 50, 300.0)
        deleted = await db.delete_stale_positions("U1", set())
        assert deleted == 2
        rows = await db.get_positions(account_name="U1")
        assert len(rows) == 0

    async def test_delete_stale_only_affects_target_account(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        await db.upsert_position("U2", "AAPL", 50, 150.0)
        deleted = await db.delete_stale_positions("U1", set())
        assert deleted == 1
        rows = await db.get_positions(account_name="U2")
        assert len(rows) == 1

    async def test_delete_stale_nothing_to_delete(self, db: Database):
        await db.upsert_position("U1", "AAPL", 100, 150.0)
        deleted = await db.delete_stale_positions("U1", {"AAPL"})
        assert deleted == 0

    async def test_delete_stale_no_positions_at_all(self, db: Database):
        deleted = await db.delete_stale_positions("U1", {"AAPL"})
        assert deleted == 0


# ===========================================================================
# upsert_account_summary
# ===========================================================================


class TestAccountSummary:
    async def test_insert_new_account(self, db: Database):
        await db.upsert_account_summary(
            "U1", nlv=50000, total_market_value=48000,
            total_unrealized_pnl=2000, base_currency="EUR",
            nlv_eur=50000, nlv_usd=55000, exchange_rate=1.1,
            net_deposits=40000, display_name="Main",
        )
        cursor = await db._db.execute(
            "SELECT * FROM account_summary WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row is not None
        r = dict(row)
        assert r["nlv"] == 50000
        assert r["base_currency"] == "EUR"
        assert r["display_name"] == "Main"

    async def test_upsert_updates_existing(self, db: Database):
        await db.upsert_account_summary("U1", nlv=50000)
        await db.upsert_account_summary("U1", nlv=55000, display_name="Updated")
        cursor = await db._db.execute(
            "SELECT COUNT(*) FROM account_summary WHERE account_name = 'U1'"
        )
        count = (await cursor.fetchone())[0]
        assert count == 1
        cursor = await db._db.execute(
            "SELECT nlv, display_name FROM account_summary WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row[0] == 55000
        assert row[1] == "Updated"

    async def test_defaults(self, db: Database):
        await db.upsert_account_summary("U1", nlv=10000)
        cursor = await db._db.execute(
            "SELECT base_currency, nlv_eur, exchange_rate, net_deposits FROM account_summary WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row[0] == "USD"
        assert row[1] == 0
        assert row[2] == 1.0
        assert row[3] == 0


# ===========================================================================
# get_todays_executions
# ===========================================================================


class TestGetTodaysExecutions:
    async def test_returns_todays_executions_with_signal(self, db: Database):
        sid = await _insert_signal(db, ticker="GOOG")
        await _insert_execution(db, sid, account_name="U1")
        rows = await db.get_todays_executions()
        assert len(rows) == 1
        assert rows[0]["ticker"] == "GOOG"
        assert rows[0]["signal_action"] == "BUY"

    async def test_empty_when_no_executions(self, db: Database):
        rows = await db.get_todays_executions()
        assert rows == []

    async def test_multiple_executions(self, db: Database):
        sid = await _insert_signal(db)
        await _insert_execution(db, sid, account_name="U1", order_id=1)
        await _insert_execution(db, sid, account_name="U2", order_id=2)
        rows = await db.get_todays_executions()
        assert len(rows) == 2


# ===========================================================================
# get_executions_since
# ===========================================================================


class TestGetExecutionsSince:
    async def test_returns_executions_after_date(self, db: Database):
        sid = await _insert_signal(db, ticker="NVDA")
        await _insert_execution(db, sid)
        yesterday = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        rows = await db.get_executions_since(yesterday)
        assert len(rows) == 1
        assert rows[0]["ticker"] == "NVDA"

    async def test_excludes_old_executions(self, db: Database):
        sid = await _insert_signal(db)
        await _insert_execution(db, sid)
        tomorrow = (datetime.now(UTC) + timedelta(days=1)).isoformat()
        rows = await db.get_executions_since(tomorrow)
        assert rows == []


# ===========================================================================
# find_recent_signal
# ===========================================================================


class TestFindRecentSignal:
    async def test_finds_matching_signal(self, db: Database):
        sid = await _insert_signal(db, ticker="AAPL", action="BUY")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", since)
        assert result is not None
        assert result["id"] == sid

    async def test_returns_none_when_no_match(self, db: Database):
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", since)
        assert result is None

    async def test_excludes_skipped_status(self, db: Database):
        sid = await _insert_signal(db, ticker="AAPL", action="BUY")
        await db.update_signal_status(sid, "skipped")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", since)
        assert result is None

    async def test_excludes_failed_status(self, db: Database):
        sid = await _insert_signal(db, ticker="AAPL", action="BUY")
        await db.update_signal_status(sid, "failed")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", since)
        assert result is None

    async def test_includes_pending_status(self, db: Database):
        await _insert_signal(db, ticker="AAPL", action="BUY")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", since)
        assert result is not None
        assert result["status"] == "pending"

    async def test_includes_executed_status(self, db: Database):
        sid = await _insert_signal(db, ticker="AAPL", action="BUY")
        await db.update_signal_status(sid, "executed")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", since)
        assert result is not None

    async def test_respects_since_cutoff(self, db: Database):
        await _insert_signal(db, ticker="AAPL", action="BUY")
        future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "BUY", future)
        assert result is None

    async def test_wrong_ticker_no_match(self, db: Database):
        await _insert_signal(db, ticker="AAPL", action="BUY")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("MSFT", "BUY", since)
        assert result is None

    async def test_wrong_action_no_match(self, db: Database):
        await _insert_signal(db, ticker="AAPL", action="BUY")
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        result = await db.find_recent_signal("AAPL", "SELL", since)
        assert result is None


# ===========================================================================
# cash transactions
# ===========================================================================


class TestCashTransactions:
    async def test_upsert_cash_transaction(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-15", 5000.0, "EUR", "Deposit")
        rows = await db.get_cash_transactions(account_name="U1")
        assert len(rows) == 1
        assert rows[0]["amount"] == 5000.0
        assert rows[0]["currency"] == "EUR"
        assert rows[0]["description"] == "Deposit"

    async def test_duplicate_ignored(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-15", 5000.0, "EUR", "Deposit")
        await db.upsert_cash_transaction("U1", "2026-01-15", 5000.0, "EUR", "Deposit")
        rows = await db.get_cash_transactions(account_name="U1")
        assert len(rows) == 1

    async def test_different_amounts_not_duplicate(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-15", 5000.0, "EUR", "Deposit")
        await db.upsert_cash_transaction("U1", "2026-01-15", 3000.0, "EUR", "Deposit")
        rows = await db.get_cash_transactions(account_name="U1")
        assert len(rows) == 2

    async def test_get_cash_transactions_no_filter(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-10", 1000.0)
        await db.upsert_cash_transaction("U2", "2026-01-20", 2000.0)
        rows = await db.get_cash_transactions()
        assert len(rows) == 2

    async def test_get_cash_transactions_filter_by_account(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-10", 1000.0)
        await db.upsert_cash_transaction("U2", "2026-01-20", 2000.0)
        rows = await db.get_cash_transactions(account_name="U1")
        assert len(rows) == 1
        assert rows[0]["account_name"] == "U1"

    async def test_get_cash_transactions_ordered_by_date(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-03-01", 3000.0, description="Mar")
        await db.upsert_cash_transaction("U1", "2026-01-01", 1000.0, description="Jan")
        await db.upsert_cash_transaction("U1", "2026-02-01", 2000.0, description="Feb")
        rows = await db.get_cash_transactions(account_name="U1")
        assert rows[0]["description"] == "Jan"
        assert rows[1]["description"] == "Feb"
        assert rows[2]["description"] == "Mar"

    async def test_default_currency(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-15", 500.0)
        rows = await db.get_cash_transactions()
        assert rows[0]["currency"] == "EUR"


class TestGetNetDeposits:
    async def test_sum_deposits(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-01", 5000.0)
        await db.upsert_cash_transaction("U1", "2026-02-01", 3000.0)
        total = await db.get_net_deposits("U1")
        assert total == 8000.0

    async def test_deposits_and_withdrawals(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-01", 5000.0, description="dep")
        await db.upsert_cash_transaction("U1", "2026-02-01", -2000.0, description="wdr")
        total = await db.get_net_deposits("U1")
        assert total == 3000.0

    async def test_zero_when_no_transactions(self, db: Database):
        total = await db.get_net_deposits("U1")
        assert total == 0

    async def test_only_sums_target_account(self, db: Database):
        await db.upsert_cash_transaction("U1", "2026-01-01", 5000.0)
        await db.upsert_cash_transaction("U2", "2026-01-01", 9000.0)
        assert await db.get_net_deposits("U1") == 5000.0
        assert await db.get_net_deposits("U2") == 9000.0


# ===========================================================================
# log_audit
# ===========================================================================


class TestLogAudit:
    async def test_log_audit_entry(self, db: Database):
        await db.log_audit("ORDER_PLACED", signal_id=1, ticker="AAPL", detail="10 shares")
        cursor = await db._db.execute("SELECT * FROM audit_log")
        rows = await cursor.fetchall()
        assert len(rows) == 1
        r = dict(rows[0])
        assert r["event"] == "ORDER_PLACED"
        assert r["signal_id"] == 1
        assert r["ticker"] == "AAPL"
        assert r["detail"] == "10 shares"
        assert r["created_at"] is not None

    async def test_log_audit_minimal(self, db: Database):
        await db.log_audit("SYNC_COMPLETE")
        cursor = await db._db.execute("SELECT * FROM audit_log")
        rows = await cursor.fetchall()
        r = dict(rows[0])
        assert r["event"] == "SYNC_COMPLETE"
        assert r["signal_id"] is None
        assert r["ticker"] is None
        assert r["detail"] == ""

    async def test_multiple_audit_entries(self, db: Database):
        await db.log_audit("A")
        await db.log_audit("B")
        await db.log_audit("C")
        cursor = await db._db.execute("SELECT COUNT(*) FROM audit_log")
        count = (await cursor.fetchone())[0]
        assert count == 3


# ===========================================================================
# snapshot_nlv
# ===========================================================================


class TestSnapshotNlv:
    async def test_inserts_snapshot(self, db: Database):
        await db.snapshot_nlv("U1", nlv_eur=50000, nlv_usd=55000, net_deposits=40000)
        cursor = await db._db.execute(
            "SELECT * FROM nlv_history WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row is not None
        r = dict(row)
        assert r["nlv_eur"] == 50000
        assert r["nlv_usd"] == 55000
        assert r["net_deposits"] == 40000
        assert r["date"] == datetime.now(UTC).strftime("%Y-%m-%d")

    async def test_upsert_same_day_updates(self, db: Database):
        await db.snapshot_nlv("U1", nlv_eur=50000, nlv_usd=55000)
        await db.snapshot_nlv("U1", nlv_eur=51000, nlv_usd=56000)
        cursor = await db._db.execute(
            "SELECT COUNT(*) FROM nlv_history WHERE account_name = 'U1'"
        )
        count = (await cursor.fetchone())[0]
        assert count == 1
        cursor = await db._db.execute(
            "SELECT nlv_eur, nlv_usd FROM nlv_history WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row[0] == 51000
        assert row[1] == 56000

    async def test_different_accounts_separate_rows(self, db: Database):
        await db.snapshot_nlv("U1", nlv_eur=50000, nlv_usd=55000)
        await db.snapshot_nlv("U2", nlv_eur=20000, nlv_usd=22000)
        cursor = await db._db.execute("SELECT COUNT(*) FROM nlv_history")
        count = (await cursor.fetchone())[0]
        assert count == 2

    async def test_default_net_deposits_zero(self, db: Database):
        await db.snapshot_nlv("U1", nlv_eur=10000, nlv_usd=11000)
        cursor = await db._db.execute(
            "SELECT net_deposits FROM nlv_history WHERE account_name = 'U1'"
        )
        row = await cursor.fetchone()
        assert row[0] == 0


# ===========================================================================
# close
# ===========================================================================


class TestClose:
    async def test_close_idempotent(self, db: Database):
        """Calling close() when already closed by fixture is fine."""
        await db.close()

    async def test_close_when_no_connection(self):
        """Close on an uninitialized Database does not raise."""
        database = Database(":memory:")
        await database.close()
