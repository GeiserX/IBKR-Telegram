"""Tests for the webhook API server."""

import unittest.mock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from src.models import TradeSignal
from src.webhook import WebhookServer


@pytest.fixture
def received_signals():
    return []


@pytest.fixture
def webhook(received_signals):
    async def on_signal(signal: TradeSignal) -> dict:
        received_signals.append(signal)
        return {"signal_id": 42, "status": "pending_confirmation"}

    return WebhookServer(secret="test-secret", port=0, on_signal=on_signal)


@pytest.fixture
async def client(webhook):
    server = TestServer(webhook._app)
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_missing_auth(client):
    resp = await client.post("/api/v1/signal", json={"ticker": "AAPL", "action": "BUY"})
    assert resp.status == 401
    data = await resp.json()
    assert data["error"] == "unauthorized"


@pytest.mark.asyncio
async def test_wrong_auth(client):
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY"},
        headers={"Authorization": "Bearer wrong"},
    )
    assert resp.status == 401
    data = await resp.json()
    assert data["error"] == "unauthorized"


@pytest.mark.asyncio
async def test_invalid_json(client):
    resp = await client.post(
        "/api/v1/signal",
        data=b"not json",
        headers={
            "Authorization": "Bearer test-secret",
            "Content-Type": "application/json",
        },
    )
    assert resp.status == 400
    data = await resp.json()
    assert data["error"] == "invalid JSON"


@pytest.mark.asyncio
async def test_missing_ticker(client):
    resp = await client.post(
        "/api/v1/signal",
        json={"action": "BUY"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "ticker" in data["error"]


@pytest.mark.asyncio
async def test_missing_action(client):
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "action" in data["error"]


@pytest.mark.asyncio
async def test_invalid_action(client):
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "YOLO"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "invalid action" in data["error"]


@pytest.mark.asyncio
async def test_trim_is_invalid_action(client):
    """TRIM is not a supported action — it has no executor handler."""
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "TRIM"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "invalid action" in data["error"]


@pytest.mark.asyncio
async def test_invalid_weight_negative(client):
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY", "target_weight_pct": -5},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "target_weight_pct" in data["error"]


@pytest.mark.asyncio
async def test_invalid_weight_bool(client):
    """Booleans are not valid for target_weight_pct even though bool is a subclass of int."""
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY", "target_weight_pct": True},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "target_weight_pct" in data["error"]


@pytest.mark.asyncio
async def test_invalid_weight_over_100(client):
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY", "target_weight_pct": 150},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "target_weight_pct" in data["error"]


@pytest.mark.asyncio
async def test_invalid_message_id_type(client):
    """message_id must be an integer, not a string."""
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY", "message_id": "abc"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "message_id" in data["error"]


@pytest.mark.asyncio
async def test_invalid_message_id_bool(client):
    """Booleans are not valid for message_id even though bool is a subclass of int."""
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY", "message_id": True},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "message_id" in data["error"]


@pytest.mark.asyncio
async def test_invalid_related_ticker_type(client):
    """related_ticker must be a string, not a number."""
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY", "related_ticker": 123},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 400
    data = await resp.json()
    assert "related_ticker" in data["error"]


@pytest.fixture
async def failing_client():
    async def failing_callback(signal: TradeSignal) -> dict:
        raise RuntimeError("DB connection lost")

    server_obj = WebhookServer(secret="test-secret", port=0, on_signal=failing_callback)
    test_server = TestServer(server_obj._app)
    client = TestClient(test_server)
    await client.start_server()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_callback_exception_returns_500(failing_client):
    """When on_signal raises, the webhook returns 500 with a structured error."""
    resp = await failing_client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "BUY"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 500
    data = await resp.json()
    assert data["error"] == "internal processing error"


@pytest.mark.asyncio
async def test_weight_zero_is_valid(client, received_signals):
    """target_weight_pct=0 means 'close position' and should be accepted."""
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "AAPL", "action": "SELL", "target_weight_pct": 0},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 202
    assert len(received_signals) == 1
    assert received_signals[0].target_weight_pct == 0


@pytest.mark.asyncio
async def test_valid_signal(client, received_signals):
    resp = await client.post(
        "/api/v1/signal",
        json={
            "ticker": "aapl",
            "action": "buy",
            "target_weight_pct": 5.0,
            "source": "test",
            "raw_text": "Buy AAPL 5%",
        },
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 202
    data = await resp.json()
    assert data["signal_id"] == 42
    assert data["status"] == "pending_confirmation"

    assert len(received_signals) == 1
    sig = received_signals[0]
    assert sig.ticker == "AAPL"
    assert sig.action == "BUY"
    assert sig.target_weight_pct == 5.0
    assert sig.source == "test"


@pytest.mark.asyncio
async def test_valid_signal_minimal(client, received_signals):
    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "MSFT", "action": "SELL"},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status == 202
    assert len(received_signals) == 1
    assert received_signals[0].ticker == "MSFT"
    assert received_signals[0].source == "webhook"


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_empty_secret_raises_value_error():
    """WebhookServer requires a non-empty secret — empty string raises ValueError."""

    async def on_signal(signal: TradeSignal) -> dict:
        return {"signal_id": 1, "status": "pending_confirmation"}

    with pytest.raises(ValueError, match="non-empty secret"):
        WebhookServer(secret="", port=0, on_signal=on_signal)


@pytest.mark.asyncio
async def test_start_and_stop():
    """WebhookServer can start and stop without error."""
    async def on_signal(signal: TradeSignal) -> dict:
        return {"signal_id": 1, "status": "pending_confirmation"}

    server = WebhookServer(secret="test-secret", port=0, on_signal=on_signal)
    await server.start()
    assert server._runner is not None
    await server.stop()


@pytest.mark.asyncio
async def test_stop_when_not_started():
    """Stopping a server that was never started is a no-op."""
    async def on_signal(signal: TradeSignal) -> dict:
        return {"signal_id": 1, "status": "pending_confirmation"}

    server = WebhookServer(secret="test-secret", port=0, on_signal=on_signal)
    assert server._runner is None
    await server.stop()  # Should not raise


@pytest.mark.asyncio
async def test_callback_timeout_returns_504():
    """When on_signal takes too long, the webhook returns 504."""
    import asyncio

    async def slow_callback(signal: TradeSignal) -> dict:
        await asyncio.sleep(60)
        return {"signal_id": 1, "status": "pending_confirmation"}

    server_obj = WebhookServer(secret="test-secret", port=0, on_signal=slow_callback)
    # Monkey-patch a shorter timeout for the test
    original_handle = server_obj._handle_signal

    async def patched_handle(request):
        # Replace the 30s timeout with 0.1s for testing
        orig_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await orig_wait_for(coro, timeout=0.1)

        with unittest.mock.patch("src.webhook.asyncio.wait_for", side_effect=fast_wait_for):
            return await original_handle(request)

    app = web.Application()
    app.router.add_post("/api/v1/signal", patched_handle)
    app.router.add_get("/health", server_obj._handle_health)

    test_server = TestServer(app)
    test_client = TestClient(test_server)
    await test_client.start_server()
    try:
        resp = await test_client.post(
            "/api/v1/signal",
            json={"ticker": "AAPL", "action": "BUY"},
            headers={"Authorization": "Bearer test-secret"},
        )
        assert resp.status == 504
        data = await resp.json()
        assert data["error"] == "processing timeout"
    finally:
        await test_client.close()


@pytest.mark.asyncio
async def test_duplicate_signal_returns_200():
    """When on_signal returns duplicate_skipped status, HTTP status is 200."""
    async def dup_callback(signal: TradeSignal) -> dict:
        return {"status": "duplicate_skipped", "ticker": signal.ticker, "action": signal.action}

    server_obj = WebhookServer(secret="test-secret", port=0, on_signal=dup_callback)
    test_server = TestServer(server_obj._app)
    dup_client = TestClient(test_server)
    await dup_client.start_server()
    try:
        resp = await dup_client.post(
            "/api/v1/signal",
            json={"ticker": "AAPL", "action": "BUY"},
            headers={"Authorization": "Bearer test-secret"},
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "duplicate_skipped"
    finally:
        await dup_client.close()
