"""Tests for the trade executor — unit tests for sizing and sell fraction logic."""

from src.executor import ExecutionResult, TradeExecutor


def test_parse_sell_fraction_small():
    assert TradeExecutor._parse_sell_fraction("small") == 0.25


def test_parse_sell_fraction_large():
    assert TradeExecutor._parse_sell_fraction("large") == 1.0


def test_parse_sell_fraction_percentage():
    assert TradeExecutor._parse_sell_fraction("10%") == 0.10
    assert TradeExecutor._parse_sell_fraction("50%") == 0.50
    assert TradeExecutor._parse_sell_fraction("100%") == 1.0


def test_parse_sell_fraction_default():
    assert TradeExecutor._parse_sell_fraction("") == 0.5
    assert TradeExecutor._parse_sell_fraction(None) == 0.5


def test_parse_sell_fraction_unknown():
    import pytest
    with pytest.raises(ValueError, match="Unrecognized sell amount"):
        TradeExecutor._parse_sell_fraction("some random text")


def test_parse_sell_fraction_case_insensitive():
    assert TradeExecutor._parse_sell_fraction("Small") == 0.25
    assert TradeExecutor._parse_sell_fraction(" LARGE ") == 1.0
    assert TradeExecutor._parse_sell_fraction("all") == 1.0
    assert TradeExecutor._parse_sell_fraction("half") == 0.5


def test_execution_result_success():
    r = ExecutionResult(account_name="test", success=True, order_id=123, filled_qty=5, avg_price=10.50)
    assert r.success
    assert r.order_id == 123
    assert r.error is None


def test_execution_result_failure():
    r = ExecutionResult(account_name="test", success=False, error="NLV unavailable")
    assert not r.success
    assert r.filled_qty == 0
    assert r.error == "NLV unavailable"
