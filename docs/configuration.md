# Configuration

## `config.yaml`

```yaml
accounts:
  - name: main
    gateway_host: ib-gateway    # Docker service name
    gateway_port: 4003          # IB Gateway API port
    max_allocation_pct: 100     # Max total portfolio allocation
    max_position_pct: 15        # Max single position size
    margin_mode: soft           # "soft", "hard", or "off"
    # max_margin_usd: 5000      # Optional margin cap in USD

trading:
  order_type: LMT               # LMT or MKT
  limit_offset_pct: 0.5         # Offset from mid for limit orders
  confirm_before_execute: true   # Require Telegram confirmation
  asset_types:
    - options
    - stocks
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_ADMIN_CHAT_ID` | Your Telegram user ID (admin only) |
| `CONFIG_PATH` | Path to config.yaml (default: `config.yaml`) |
| `WEBHOOK_SECRET` | Secret for webhook Bearer auth (optional, enables webhook API) |
| `WEBHOOK_PORT` | Webhook server port (default: `8080`) |
| `MARGIN_MODE_<NAME>` | Per-account margin mode override |
| `MAX_MARGIN_<NAME>` | Per-account margin cap override (USD) |

## Margin modes

| Mode | Behavior |
|------|----------|
| `off` | No margin awareness — sizes based on cash only |
| `soft` | Uses margin for position sizing, sends alerts when approaching limits |
| `hard` | Soft behavior + automatically sells positions when margin limits are breached |

## IB Gateway

This project uses the [gnzsnz/ib-gateway-docker](https://github.com/gnzsnz/ib-gateway-docker) Docker image. Key settings:

- **2FA**: Required once per week (Sunday). The container auto-restarts and relogins after timeout.
- **Session persistence**: `SAVE_TWS_SETTINGS=yes` preserves settings across restarts.
- **API access**: `READ_ONLY_API=no` is required for order execution.

See [`docker-compose.example.yml`](https://github.com/GeiserX/IBKR-Telegram/blob/main/docker-compose.example.yml) for the full configuration.
