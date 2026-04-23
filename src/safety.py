"""Pre-execution safety checks — market hours, position limits, duplicate detection."""

import logging
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# US Eastern timezone for market hours
ET = ZoneInfo("America/New_York")

# Regular market hours (options)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0

# Duplicate detection window (hours)
DUPLICATE_WINDOW_HOURS = 4


def is_market_open() -> bool:
    """Check if US equity/options markets are currently open (regular session).

    Returns True during 9:30-16:00 ET on weekdays.
    Does not account for holidays — orders will be rejected by IBKR anyway.
    """
    now_et = datetime.now(ET)

    # Weekday check (0=Monday, 6=Sunday)
    if now_et.weekday() >= 5:
        return False

    market_open = now_et.replace(
        hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
    )
    market_close = now_et.replace(
        hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0
    )

    return market_open <= now_et < market_close


def time_until_market_open() -> timedelta | None:
    """Return time until next market open, or None if market is open now."""
    if is_market_open():
        return None

    now_et = datetime.now(ET)

    # Find next weekday
    next_open = now_et.replace(
        hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0
    )

    if now_et >= next_open or now_et.weekday() >= 5:
        # Move to next day
        next_open += timedelta(days=1)

    # Skip weekends
    while next_open.weekday() >= 5:
        next_open += timedelta(days=1)

    return next_open - now_et


async def check_duplicate_signal(db, ticker: str, action: str) -> bool:
    """Check if a similar signal was already processed recently.

    Returns True if a duplicate is found (signal should be skipped).
    """
    if not db:
        return False

    cutoff = (datetime.now(UTC) - timedelta(hours=DUPLICATE_WINDOW_HOURS)).isoformat()
    recent = await db.find_recent_signal(ticker, action, cutoff)

    if recent:
        logger.warning(
            f"Duplicate signal detected: {action} ${ticker} — "
            f"already processed as signal #{recent['id']} "
            f"at {recent['created_at']}"
        )
        return True

    return False


def check_position_limits(
    signal_action: str,
    signal_ticker: str,
    target_pct: float,
    max_position_pct: float,
    max_allocation_pct: float,
    current_positions: list[dict],
) -> str | None:
    """Validate that a trade won't breach position or allocation limits.

    Returns None if OK, or an error message string if limit would be breached.
    """
    if signal_action == "SELL":
        return None  # Sells always reduce exposure

    # Check single-position limit
    if target_pct > max_position_pct:
        return (
            f"Target {target_pct}% for ${signal_ticker} exceeds "
            f"max position limit {max_position_pct}%"
        )

    # Check total exposure limit
    total_weight = sum(p.get("weight_pct", 0) or 0 for p in current_positions)

    # For buys: new total = current total + new position weight
    existing = next(
        (p for p in current_positions if p.get("ticker") == signal_ticker), None
    )
    if existing:
        projected_total = total_weight - (existing.get("weight_pct", 0) or 0) + target_pct
    else:
        projected_total = total_weight + target_pct

    if projected_total > max_allocation_pct:
        return (
            f"Adding {target_pct}% ${signal_ticker} would bring total exposure "
            f"to {projected_total:.1f}%, exceeding max {max_allocation_pct}%"
        )

    return None
