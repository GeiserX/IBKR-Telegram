# Webhook API

The bot optionally exposes an HTTP endpoint for receiving trade signals from external sources such as TradingView alerts, custom parsers, or other trading systems.

**Endpoint:** `POST /api/v1/signal`

**Authentication:** Bearer token via the `Authorization` header. Set `WEBHOOK_SECRET` in your environment to enable the webhook server.

**Signal flow:** Incoming signals are saved to the database and presented to the admin in Telegram for confirmation before execution — the same confirmation gate as manual commands.

**Health check:** `GET /health` returns `{"status": "ok"}`.

**Example request:**

```bash
curl -X POST http://localhost:8080/api/v1/signal \
  -H "Authorization: Bearer YOUR_WEBHOOK_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"ticker": "AAPL", "action": "BUY", "target_weight_pct": 5}'
```
