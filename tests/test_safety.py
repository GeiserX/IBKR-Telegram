"""Tests for safety checks — market hours, position limits, duplicate detection."""

from unittest.mock import AsyncMock, patch

import pytest

from src.safety import check_duplicate_signal, check_position_limits, is_market_open

# === Market Hours ===


def _mock_et_datetime(weekday, hour, minute):
    """Create a mock datetime.now(ET) for testing."""
    # Build a date that falls on the desired weekday (0=Mon)
    # 2026-04-20 is Monday
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    base = datetime(2026, 4, 20, hour, minute, tzinfo=ZoneInfo("America/New_York"))
    target = base + timedelta(days=weekday)  # shift to desired weekday
    return target


@patch("src.safety.datetime")
def test_market_open_weekday_during_hours(mock_dt):
    mock_dt.now.return_value = _mock_et_datetime(0, 10, 0)  # Monday 10:00
    mock_dt.side_effect = lambda *a, **kw: mock_dt
    assert is_market_open() is True


@patch("src.safety.datetime")
def test_market_closed_weekend(mock_dt):
    mock_dt.now.return_value = _mock_et_datetime(5, 12, 0)  # Saturday 12:00
    assert is_market_open() is False


@patch("src.safety.datetime")
def test_market_closed_before_open(mock_dt):
    mock_dt.now.return_value = _mock_et_datetime(1, 8, 0)  # Tuesday 8:00
    mock_dt.side_effect = lambda *a, **kw: mock_dt
    assert is_market_open() is False


@patch("src.safety.datetime")
def test_market_closed_after_close(mock_dt):
    mock_dt.now.return_value = _mock_et_datetime(2, 16, 30)  # Wednesday 16:30
    mock_dt.side_effect = lambda *a, **kw: mock_dt
    assert is_market_open() is False


# === Position Limits ===


def test_position_limit_sell_always_ok():
    result = check_position_limits("SELL", "IREN", 50.0, 15.0, 100.0, [])
    assert result is None


def test_position_limit_exceeds_max_position():
    result = check_position_limits("BUY", "IREN", 20.0, 15.0, 100.0, [])
    assert result is not None
    assert "max position limit" in result


def test_position_limit_within_bounds():
    positions = [
        {"ticker": "CIFR", "weight_pct": 10.0},
        {"ticker": "BTDR", "weight_pct": 8.0},
    ]
    result = check_position_limits("BUY", "IREN", 12.0, 15.0, 100.0, positions)
    assert result is None


def test_position_limit_exceeds_total_allocation():
    positions = [
        {"ticker": "CIFR", "weight_pct": 40.0},
        {"ticker": "BTDR", "weight_pct": 35.0},
        {"ticker": "HIVE", "weight_pct": 20.0},
    ]
    result = check_position_limits("BUY", "IREN", 10.0, 15.0, 100.0, positions)
    assert result is not None
    assert "total exposure" in result


def test_position_limit_adding_to_existing():
    positions = [
        {"ticker": "IREN", "weight_pct": 10.0},
        {"ticker": "CIFR", "weight_pct": 40.0},
    ]
    # Increasing IREN from 10% to 14% — total goes from 50% to 54%, within 100%
    result = check_position_limits("BUY", "IREN", 14.0, 15.0, 100.0, positions)
    assert result is None


# === Duplicate Detection ===


@pytest.mark.asyncio
async def test_duplicate_detected():
    db = AsyncMock()
    db.find_recent_signal.return_value = {"id": 42, "created_at": "2026-04-18T10:00:00"}
    assert await check_duplicate_signal(db, "IREN", "BUY") is True


@pytest.mark.asyncio
async def test_no_duplicate():
    db = AsyncMock()
    db.find_recent_signal.return_value = None
    assert await check_duplicate_signal(db, "IREN", "BUY") is False


@pytest.mark.asyncio
async def test_duplicate_no_db():
    assert await check_duplicate_signal(None, "IREN", "BUY") is False
