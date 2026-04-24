"""Webhook API for receiving trade signals from external sources."""

import asyncio
import hmac
import logging
from datetime import UTC, datetime

from aiohttp import web

from .models import TradeSignal

logger = logging.getLogger(__name__)

VALID_ACTIONS = {"BUY", "SELL", "ROLL"}


class WebhookServer:
    """HTTP server that accepts trade signals via POST and exposes a health endpoint."""

    def __init__(self, secret: str, port: int, on_signal):
        if not secret:
            raise ValueError("WebhookServer requires a non-empty secret")
        self._secret = secret
        self._port = port
        self._on_signal = on_signal  # async callback(TradeSignal) -> dict
        self._app = web.Application(client_max_size=64 * 1024)
        self._app.router.add_post("/api/v1/signal", self._handle_signal)
        self._app.router.add_get("/health", self._handle_health)
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        logger.info("Webhook server listening on port %d", self._port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()

    def _check_auth(self, request: web.Request) -> bool:
        auth = request.headers.get("Authorization", "")
        return hmac.compare_digest(auth, f"Bearer {self._secret}")

    async def _handle_signal(self, request: web.Request) -> web.Response:
        if not self._check_auth(request):
            return web.json_response({"error": "unauthorized"}, status=401)

        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        ticker = (data.get("ticker") or "").strip().upper()
        action = (data.get("action") or "").strip().upper()

        if not ticker or not action:
            return web.json_response(
                {"error": "ticker and action are required"}, status=400,
            )
        if action not in VALID_ACTIONS:
            return web.json_response(
                {"error": f"invalid action: {action}"}, status=400,
            )

        weight = data.get("target_weight_pct")
        if weight is not None:
            if isinstance(weight, bool) or not isinstance(weight, (int, float)) or weight < 0 or weight > 100:
                return web.json_response(
                    {"error": "target_weight_pct must be a number between 0 and 100"},
                    status=400,
                )

        message_id = data.get("message_id")
        if message_id is not None and not isinstance(message_id, int):
            return web.json_response(
                {"error": "message_id must be an integer"}, status=400,
            )

        related_ticker = data.get("related_ticker")
        if related_ticker is not None and not isinstance(related_ticker, str):
            return web.json_response(
                {"error": "related_ticker must be a string"}, status=400,
            )

        signal = TradeSignal(
            ticker=ticker,
            action=action,
            target_weight_pct=weight,
            amount_description=data.get("amount_description", ""),
            related_ticker=related_ticker,
            raw_text=data.get("raw_text", ""),
            source=data.get("source", "webhook"),
            timestamp=datetime.now(UTC),
            message_id=message_id,
        )

        try:
            result = await asyncio.wait_for(self._on_signal(signal), timeout=30)
        except TimeoutError:
            logger.warning("Signal processing timed out")
            return web.json_response({"error": "processing timeout"}, status=504)
        except Exception:
            logger.exception("Signal processing failed")
            return web.json_response(
                {"error": "internal processing error"}, status=500,
            )
        return web.json_response(result, status=202)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})
