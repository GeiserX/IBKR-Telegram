"""SQLite database for trade signals, executions, and audit log."""

import logging
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_message_id INTEGER,
    ticker TEXT NOT NULL,
    action TEXT NOT NULL,
    target_weight_pct REAL,
    amount_description TEXT,
    related_ticker TEXT,
    raw_text TEXT,
    source TEXT DEFAULT 'text',
    status TEXT DEFAULT 'pending',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER NOT NULL REFERENCES signals(id),
    account_name TEXT NOT NULL,
    order_id INTEGER,
    filled_qty INTEGER DEFAULT 0,
    avg_price REAL DEFAULT 0,
    target_allocation_pct REAL,
    actual_allocation_pct REAL,
    deviation_pct REAL,
    status TEXT DEFAULT 'pending',
    error TEXT,
    created_at TEXT NOT NULL,
    filled_at TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    avg_cost REAL NOT NULL,
    current_price REAL,
    weight_pct REAL,
    pnl REAL,
    updated_at TEXT NOT NULL,
    UNIQUE(account_name, ticker)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    signal_id INTEGER,
    ticker TEXT,
    detail TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS account_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL UNIQUE,
    nlv REAL NOT NULL DEFAULT 0,
    total_market_value REAL DEFAULT 0,
    total_unrealized_pnl REAL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cash_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL,
    report_date TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'EUR',
    description TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(account_name, report_date, amount, description)
);

CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_executions_signal ON executions(signal_id);
CREATE INDEX IF NOT EXISTS idx_positions_account ON positions(account_name);
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_cash_txn_account ON cash_transactions(account_name);

CREATE TABLE IF NOT EXISTS nlv_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name TEXT NOT NULL,
    date TEXT NOT NULL,
    nlv_eur REAL NOT NULL,
    nlv_usd REAL NOT NULL,
    net_deposits REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(account_name, date)
);

CREATE INDEX IF NOT EXISTS idx_nlv_history_account_date ON nlv_history(account_name, date);
"""


class Database:
    """Async SQLite database manager."""

    def __init__(self, db_path: str = "data/trades.db"):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        """Initialize database and create tables."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA foreign_keys = ON")
        await self._db.execute("PRAGMA journal_mode=WAL")
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA)
        # Migrations: add columns for multi-currency and deposit tracking
        for sql in [
            "ALTER TABLE account_summary ADD COLUMN base_currency TEXT DEFAULT 'USD'",
            "ALTER TABLE account_summary ADD COLUMN nlv_eur REAL DEFAULT 0",
            "ALTER TABLE account_summary ADD COLUMN nlv_usd REAL DEFAULT 0",
            "ALTER TABLE account_summary ADD COLUMN exchange_rate REAL DEFAULT 1.0",
            "ALTER TABLE account_summary ADD COLUMN net_deposits REAL DEFAULT 0",
            "ALTER TABLE account_summary ADD COLUMN display_name TEXT DEFAULT ''",
        ]:
            try:
                await self._db.execute(sql)
            except Exception as e:
                if "duplicate column" not in str(e).lower():
                    logger.warning(f"Migration warning: {e}")
        await self._db.commit()
        logger.info(f"Database initialized at {self.db_path} (WAL mode)")

    async def close(self) -> None:
        """Close database connection."""
        if self._db:
            await self._db.close()

    async def save_signal(
        self,
        message_id: int,
        ticker: str,
        action: str,
        target_weight_pct: float | None,
        amount_description: str,
        related_ticker: str | None,
        raw_text: str,
        source: str,
    ) -> int:
        """Save a parsed signal and return its ID."""
        cursor = await self._db.execute(
            """INSERT INTO signals
               (channel_message_id, ticker, action, target_weight_pct,
                amount_description, related_ticker, raw_text, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, ticker, action, target_weight_pct,
             amount_description, related_ticker, raw_text, source,
             datetime.now(UTC).isoformat()),
        )
        await self._db.commit()
        return cursor.lastrowid

    async def save_execution(
        self,
        signal_id: int,
        account_name: str,
        order_id: int | None,
        filled_qty: int,
        avg_price: float,
        target_pct: float,
        actual_pct: float,
        status: str,
        error: str | None = None,
    ) -> int:
        """Save an execution result."""
        deviation = abs(target_pct - actual_pct) if actual_pct else 0
        cursor = await self._db.execute(
            """INSERT INTO executions
               (signal_id, account_name, order_id, filled_qty, avg_price,
                target_allocation_pct, actual_allocation_pct, deviation_pct,
                status, error, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (signal_id, account_name, order_id, filled_qty, avg_price,
             target_pct, actual_pct, deviation, status, error,
             datetime.now(UTC).isoformat()),
        )
        await self._db.commit()
        return cursor.lastrowid

    async def update_signal_status(self, signal_id: int, status: str) -> None:
        """Update signal status (pending, confirmed, executed, skipped, failed)."""
        await self._db.execute(
            "UPDATE signals SET status = ? WHERE id = ?",
            (status, signal_id),
        )
        await self._db.commit()

    async def update_execution_fill(
        self,
        execution_id: int,
        filled_qty: int,
        avg_price: float,
        status: str,
    ) -> None:
        """Update execution after a fill event."""
        await self._db.execute(
            """UPDATE executions
               SET filled_qty = ?, avg_price = ?, status = ?,
                   filled_at = ?
               WHERE id = ?""",
            (filled_qty, avg_price, status,
             datetime.now(UTC).isoformat(), execution_id),
        )
        await self._db.commit()

    async def update_execution_allocation(
        self, execution_id: int, actual_pct: float,
    ) -> None:
        """Update actual allocation percentage after a fill."""
        await self._db.execute(
            "UPDATE executions SET actual_allocation_pct = ?, "
            "deviation_pct = ABS(target_allocation_pct - ?) WHERE id = ?",
            (actual_pct, actual_pct, execution_id),
        )
        await self._db.commit()

    async def find_execution_by_order(
        self, account_name: str, order_id: int,
    ) -> dict | None:
        """Find an execution record by account + order_id."""
        cursor = await self._db.execute(
            """SELECT * FROM executions
               WHERE account_name = ? AND order_id = ?
               ORDER BY created_at DESC LIMIT 1""",
            (account_name, order_id),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_last_sync_time(self) -> str | None:
        """Get the timestamp of the most recent account sync."""
        cursor = await self._db.execute(
            "SELECT MAX(updated_at) as last FROM account_summary"
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else None

    async def update_account_deposits(
        self, account_name: str, net_deposits: float,
    ) -> None:
        """Update net_deposits in account_summary."""
        await self._db.execute(
            "UPDATE account_summary SET net_deposits = ? WHERE account_name = ?",
            (net_deposits, account_name),
        )
        await self._db.commit()

    async def upsert_position(
        self,
        account_name: str,
        ticker: str,
        quantity: int,
        avg_cost: float,
        current_price: float | None = None,
        weight_pct: float | None = None,
        pnl: float | None = None,
    ) -> None:
        """Insert or update a position."""
        await self._db.execute(
            """INSERT INTO positions
               (account_name, ticker, quantity, avg_cost, current_price,
                weight_pct, pnl, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(account_name, ticker) DO UPDATE SET
                   quantity = COALESCE(excluded.quantity, positions.quantity),
                   avg_cost = COALESCE(excluded.avg_cost, positions.avg_cost),
                   current_price = COALESCE(excluded.current_price, positions.current_price),
                   weight_pct = COALESCE(excluded.weight_pct, positions.weight_pct),
                   pnl = COALESCE(excluded.pnl, positions.pnl),
                   updated_at = excluded.updated_at""",
            (account_name, ticker, quantity, avg_cost, current_price,
             weight_pct, pnl, datetime.now(UTC).isoformat()),
        )
        await self._db.commit()

    async def upsert_account_summary(
        self,
        account_name: str,
        nlv: float,
        total_market_value: float = 0,
        total_unrealized_pnl: float = 0,
        base_currency: str = "USD",
        nlv_eur: float = 0,
        nlv_usd: float = 0,
        exchange_rate: float = 1.0,
        net_deposits: float = 0,
        display_name: str = "",
    ) -> None:
        """Insert or update account summary (NLV, market value, PnL)."""
        await self._db.execute(
            """INSERT INTO account_summary
               (account_name, nlv, total_market_value, total_unrealized_pnl,
                base_currency, nlv_eur, nlv_usd, exchange_rate,
                net_deposits, display_name, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(account_name) DO UPDATE SET
                   nlv = excluded.nlv,
                   total_market_value = excluded.total_market_value,
                   total_unrealized_pnl = excluded.total_unrealized_pnl,
                   base_currency = excluded.base_currency,
                   nlv_eur = excluded.nlv_eur,
                   nlv_usd = excluded.nlv_usd,
                   exchange_rate = excluded.exchange_rate,
                   net_deposits = excluded.net_deposits,
                   display_name = excluded.display_name,
                   updated_at = excluded.updated_at""",
            (account_name, nlv, total_market_value, total_unrealized_pnl,
             base_currency, nlv_eur, nlv_usd, exchange_rate,
             net_deposits, display_name, datetime.now(UTC).isoformat()),
        )
        await self._db.commit()

    async def get_todays_executions(self) -> list[dict]:
        """Get all executions created today (UTC)."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        cursor = await self._db.execute(
            """SELECT e.*, s.ticker, s.action as signal_action
               FROM executions e
               JOIN signals s ON e.signal_id = s.id
               WHERE e.created_at >= ?
               ORDER BY e.created_at DESC""",
            (today,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_executions_since(self, since: str) -> list[dict]:
        """Get executions since a given ISO date string."""
        cursor = await self._db.execute(
            """SELECT e.*, s.ticker, s.action as signal_action
               FROM executions e
               JOIN signals s ON e.signal_id = s.id
               WHERE e.created_at >= ?
               ORDER BY e.created_at DESC""",
            (since,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_recent_signals(self, limit: int = 20) -> list[dict]:
        """Get recent signals for dashboard."""
        cursor = await self._db.execute(
            "SELECT * FROM signals ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_positions(self, account_name: str | None = None) -> list[dict]:
        """Get current positions, optionally filtered by account."""
        if account_name:
            cursor = await self._db.execute(
                "SELECT * FROM positions WHERE account_name = ? ORDER BY ticker",
                (account_name,),
            )
        else:
            cursor = await self._db.execute(
                "SELECT * FROM positions ORDER BY account_name, ticker"
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def find_recent_signal(
        self, ticker: str, action: str, since: str
    ) -> dict | None:
        """Find a recent signal with same ticker+action since a cutoff time."""
        cursor = await self._db.execute(
            """SELECT * FROM signals
               WHERE ticker = ? AND action = ? AND created_at >= ?
                 AND status NOT IN ('skipped', 'failed')
               ORDER BY created_at DESC LIMIT 1""",
            (ticker, action, since),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def upsert_cash_transaction(
        self,
        account_name: str,
        report_date: str,
        amount: float,
        currency: str = "EUR",
        description: str = "",
    ) -> None:
        """Insert a deposit/withdrawal record (ignore if duplicate)."""
        await self._db.execute(
            """INSERT OR IGNORE INTO cash_transactions
               (account_name, report_date, amount, currency, description, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (account_name, report_date, amount, currency, description,
             datetime.now(UTC).isoformat()),
        )
        await self._db.commit()

    async def get_net_deposits(self, account_name: str) -> float:
        """Sum all stored deposit/withdrawal amounts for an account."""
        cursor = await self._db.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM cash_transactions WHERE account_name = ?",
            (account_name,),
        )
        row = await cursor.fetchone()
        return row[0]

    async def get_cash_transactions(
        self, account_name: str | None = None
    ) -> list[dict]:
        """Get all deposit/withdrawal transactions, optionally filtered by account."""
        if account_name:
            cursor = await self._db.execute(
                "SELECT * FROM cash_transactions WHERE account_name = ? ORDER BY report_date",
                (account_name,),
            )
        else:
            cursor = await self._db.execute(
                "SELECT * FROM cash_transactions ORDER BY report_date"
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def delete_stale_positions(
        self, account_name: str, active_tickers: set[str],
    ) -> int:
        """Remove positions no longer held by an account. Returns count deleted."""
        if not active_tickers:
            cursor = await self._db.execute(
                "DELETE FROM positions WHERE account_name = ?",
                (account_name,),
            )
        else:
            # Safe: placeholders only contains "?" chars; all values are parameterized.
            placeholders = ",".join("?" for _ in active_tickers)
            cursor = await self._db.execute(
                f"DELETE FROM positions WHERE account_name = ? AND ticker NOT IN ({placeholders})",
                (account_name, *active_tickers),
            )
        await self._db.commit()
        return cursor.rowcount

    async def snapshot_nlv(
        self,
        account_name: str,
        nlv_eur: float,
        nlv_usd: float,
        net_deposits: float = 0,
    ) -> None:
        """Record a daily NLV snapshot (one per account per day)."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        await self._db.execute(
            """INSERT INTO nlv_history
               (account_name, date, nlv_eur, nlv_usd, net_deposits, created_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(account_name, date) DO UPDATE SET
                   nlv_eur = excluded.nlv_eur,
                   nlv_usd = excluded.nlv_usd,
                   net_deposits = excluded.net_deposits,
                   created_at = excluded.created_at""",
            (account_name, today, nlv_eur, nlv_usd, net_deposits,
             datetime.now(UTC).isoformat()),
        )
        await self._db.commit()

    async def log_audit(
        self,
        event: str,
        signal_id: int | None = None,
        ticker: str | None = None,
        detail: str = "",
    ) -> None:
        """Write an entry to the audit log."""
        await self._db.execute(
            """INSERT INTO audit_log (event, signal_id, ticker, detail, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (event, signal_id, ticker, detail, datetime.now(UTC).isoformat()),
        )
        await self._db.commit()
