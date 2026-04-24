<p align="center">
  <img src="banners/banner.svg" alt="IBKR-Telegram" width="100%">
</p>

# IBKR-Telegram

[![CI](https://img.shields.io/github/actions/workflow/status/GeiserX/IBKR-Telegram/ci.yml?style=flat-square&logo=github&label=CI)](https://github.com/GeiserX/IBKR-Telegram/actions/workflows/ci.yml)
[![Codecov](https://img.shields.io/codecov/c/github/GeiserX/IBKR-Telegram?style=flat-square&logo=codecov&logoColor=white)](https://app.codecov.io/gh/GeiserX/IBKR-Telegram)
[![Docker](https://img.shields.io/docker/v/drumsergio/ibkr-telegram?style=flat-square&logo=docker&logoColor=white&label=Docker&sort=semver)](https://hub.docker.com/r/drumsergio/ibkr-telegram)
[![Docker Pulls](https://img.shields.io/docker/pulls/drumsergio/ibkr-telegram?style=flat-square&logo=docker&logoColor=white)](https://hub.docker.com/r/drumsergio/ibkr-telegram)
[![Docker Image Size](https://img.shields.io/docker/image-size/drumsergio/ibkr-telegram?style=flat-square&logo=docker&logoColor=white&sort=semver)](https://hub.docker.com/r/drumsergio/ibkr-telegram)
[![Python](https://img.shields.io/badge/Python-3.12%2B-blue?style=flat-square&logo=python&logoColor=white)](https://www.python.org)

Self-hosted Telegram bot for Interactive Brokers. Manage your portfolio, execute trades, and monitor positions — all from Telegram.

## Disclaimer

This software is provided "as is" under the [GPL-3.0 license](LICENSE). **Use at your own risk.** The author(s) accept no liability for financial losses. This is not financial advice — test thoroughly with paper trading accounts before using real money.

## What It Does

- **Multi-account trading** — manage multiple IBKR accounts simultaneously from a single bot instance. Each account connects to its own IB Gateway container. The same percentage allocation is applied proportionally across all accounts, with per-account settings for position limits, margin modes, and display names — all configured in `config.yaml`.
- **Configurable instrument support** — trade any instrument supported by IBKR, including stocks, options, futures, and more. The `asset_types` field in config controls which instruments are active. A built-in option chain wizard provides LEAPS selection with deep ITM strike matching and automatic contract resolution.
- **Safety-first execution** — market hours checks, duplicate signal detection, position limit enforcement, and mandatory confirmation before every trade.
- **Real-time portfolio sync** — periodic position snapshots, P&L tracking, and Flex Web Service integration for deposit/withdrawal history.
- **Margin compliance** — configurable soft (alert) or hard (auto-sell) margin enforcement per account.
- **Webhook API** — optional HTTP endpoint for receiving trade signals from external sources (TradingView, custom parsers, etc.).

## Features

| Command | Description |
|---------|-------------|
| `/v` | Full portfolio snapshot with NLV, positions, and P&L |
| `/buy TICKER PCT PRICE\|MKT` | Add to an existing position |
| `/sell TICKER all\|half\|% PRICE\|MKT` | Reduce or close a position |
| `/new` | Open a new position via option chain wizard |
| `/info` | Position details: bid/ask, Greeks, P&L |
| `/price TICKER` | Live stock + option quote |
| `/orders` | View and cancel open orders |
| `/trades` | Execution history (today/week) |
| `/kill` | Cancel all open orders |
| `/deposits` | Deposit/withdrawal history (via IBKR Flex) |
| `/signals` | Recent signal history |
| `/status` | System health and connectivity |
| `/pending` | Pending trade confirmations |
| `/pause` | Pause IB Gateway containers (for manual IBKR login) |

## Prerequisites

- An [Interactive Brokers](https://www.interactivebrokers.com/) account with API access enabled
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- Docker and Docker Compose

## Quick Start

1. **Copy the example files:**

   ```bash
   cp .env.example .env
   cp config.example.yaml config.yaml
   cp docker-compose.example.yml docker-compose.yml
   ```

2. **Edit `.env`** with your Telegram bot token, admin chat ID, and IBKR credentials.

3. **Edit `config.yaml`** with your account names, gateway hosts, and trading preferences.

4. **Start the stack:**

   ```bash
   docker compose up -d
   ```

5. **Complete IB Gateway 2FA** — on first start, the gateway container will wait for your two-factor authentication. Check container logs for instructions.

6. **Send `/status`** in Telegram to verify connectivity.

## Configuration

### `config.yaml`

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

### Environment Variables

| Variable | Description |
|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_ADMIN_CHAT_ID` | Your Telegram user ID (admin only) |
| `CONFIG_PATH` | Path to config.yaml (default: `config.yaml`) |
| `WEBHOOK_SECRET` | Secret for webhook Bearer auth (optional, enables webhook API) |
| `WEBHOOK_PORT` | Webhook server port (default: `8080`) |
| `MARGIN_MODE_<NAME>` | Per-account margin mode override |
| `MAX_MARGIN_<NAME>` | Per-account margin cap override (USD) |

### Margin Modes

| Mode | Behavior |
|------|----------|
| `off` | No margin awareness — sizes based on cash only |
| `soft` | Uses margin for position sizing, sends alerts when approaching limits |
| `hard` | Soft behavior + automatically sells positions when margin limits are breached |

## Webhook API

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

## IB Gateway

This project uses the [gnzsnz/ib-gateway](https://github.com/gnzsnz/ib-gateway) Docker image. Key settings:

- **2FA**: Required once per week (Sunday). The container auto-restarts and relogins after timeout.
- **Session persistence**: `SAVE_TWS_SETTINGS=yes` preserves settings across restarts.
- **API access**: `READ_ONLY_API=no` is required for order execution.

See `docker-compose.example.yml` for the full configuration.

## Architecture

```
Telegram <-> Bot (aiogram) <-> TradeExecutor <-> IB Gateway (ib-async) <-> IBKR
                |                    |
             SQLite DB          Safety Checks
           (trades.db)      (hours, limits, dupes)
                ^
         Webhook API  <-  External Sources (optional)
       (POST /api/v1/signal)
```

- **Bot** (`bot.py`): Telegram command handling, inline keyboards, confirmation flows
- **Executor** (`executor.py`): IBKR connection management, order sizing, option chain resolution
- **Safety** (`safety.py`): Market hours, position limits, duplicate detection
- **DB** (`db.py`): Trade log, signal history, deposit tracking
- **Webhook** (`webhook.py`): HTTP server for external trade signals with Bearer auth
- **App** (`app.py`): Orchestration — wires bot, executor, DB, webhook, and periodic sync

## Contributing

Contributions are welcome. Please open an issue first to discuss what you'd like to change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Run tests (`pytest tests/ -v`)
4. Run linting (`ruff check .`)
5. Open a pull request

## License

[GPL-3.0](LICENSE)
