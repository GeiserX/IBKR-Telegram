"""Tests for the webhook API server."""

import pytest
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
async def test_callback_exception_returns_500():
    """When on_signal raises, the webhook returns 500 with a structured error."""

    async def failing_callback(signal: TradeSignal) -> dict:
        raise RuntimeError("DB connection lost")

    server_obj = WebhookServer(secret="test-secret", port=0, on_signal=failing_callback)
    test_server = TestServer(server_obj._app)
    client = TestClient(test_server)
    await client.start_server()
    try:
        resp = await client.post(
            "/api/v1/signal",
            json={"ticker": "AAPL", "action": "BUY"},
            headers={"Authorization": "Bearer test-secret"},
        )
        assert resp.status == 500
        data = await resp.json()
        assert data["error"] == "internal processing error"
    finally:
        await client.close()


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
async def test_no_auth_when_no_secret(received_signals):
    """When no secret is configured, requests are accepted without auth."""

    async def on_signal(signal: TradeSignal) -> dict:
        received_signals.append(signal)
        return {"signal_id": 1, "status": "pending_confirmation"}

    server = WebhookServer(secret="", port=0, on_signal=on_signal)
    test_server = TestServer(server._app)
    client = TestClient(test_server)
    await client.start_server()

    resp = await client.post(
        "/api/v1/signal",
        json={"ticker": "GOOG", "action": "BUY"},
    )
    assert resp.status == 202
    assert len(received_signals) == 1

    await client.close()
