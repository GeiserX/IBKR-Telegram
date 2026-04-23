# IBKR-Telegram

[![CI](https://github.com/GeiserX/IBKR-Telegram/actions/workflows/ci.yml/badge.svg)](https://github.com/GeiserX/IBKR-Telegram/actions/workflows/ci.yml)
[![Tests](https://github.com/GeiserX/IBKR-Telegram/actions/workflows/tests.yml/badge.svg)](https://github.com/GeiserX/IBKR-Telegram/actions/workflows/tests.yml)
[![Docker](https://img.shields.io/docker/v/drumsergio/ibkr-telegram?label=Docker&sort=semver)](https://hub.docker.com/r/drumsergio/ibkr-telegram)
[![License: GPL-3.0](https://img.shields.io/github/license/GeiserX/IBKR-Telegram)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/GeiserX/IBKR-Telegram)](https://github.com/GeiserX/IBKR-Telegram/stargazers)

Self-hosted Telegram bot for Interactive Brokers. Manage your portfolio, execute trades, and monitor positions — all from Telegram.

## What It Does

- **Multi-account trading** — manage multiple IBKR accounts from a single bot, with per-account allocation and position limits
- **Options-first** — built-in option chain wizard for LEAPS, with deep ITM strike selection and automatic contract matching
- **Safety-first execution** — market hours checks, duplicate signal detection, position limit enforcement, and mandatory confirmation before every trade
- **Real-time portfolio sync** — periodic position snapshots, P&L tracking, and Flex Web Service integration for deposit/withdrawal history
- **Margin compliance** — configurable soft (alert) or hard (auto-sell) margin enforcement per account

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
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | Bot token from @BotFather |
| `TELEGRAM_ADMIN_CHAT_ID` | Your Telegram user ID (admin only) |
| `CONFIG_PATH` | Path to config.yaml (default: `config.yaml`) |
| `MARGIN_MODE_<NAME>` | Per-account margin mode override |
| `MAX_MARGIN_<NAME>` | Per-account margin cap override (USD) |

### Margin Modes

| Mode | Behavior |
|------|----------|
| `off` | No margin awareness — sizes based on cash only |
| `soft` | Uses margin for position sizing, sends alerts when approaching limits |
| `hard` | Soft behavior + automatically sells positions when margin limits are breached |

## IB Gateway

This project uses the [gnzsnz/ib-gateway](https://github.com/gnzsnz/ib-gateway) Docker image. Key settings:

- **2FA**: Required once per week (Sunday). The container auto-restarts and relogins after timeout.
- **Session persistence**: `SAVE_TWS_SETTINGS=yes` preserves settings across restarts.
- **API access**: `READ_ONLY_API=no` is required for order execution.

See `docker-compose.example.yml` for the full configuration.

## Architecture

```
Telegram ←→ Bot (aiogram) ←→ TradeExecutor ←→ IB Gateway (ib-async) ←→ IBKR
                ↕                    ↕
             SQLite DB          Safety Checks
           (trades.db)      (hours, limits, dupes)
```

- **Bot** (`bot.py`): Telegram command handling, inline keyboards, confirmation flows
- **Executor** (`executor.py`): IBKR connection management, order sizing, option chain resolution
- **Safety** (`safety.py`): Market hours, position limits, duplicate detection
- **DB** (`db.py`): Trade log, signal history, deposit tracking
- **App** (`app.py`): Orchestration — wires bot, executor, DB, and periodic sync

## Contributing

Contributions are welcome. Please open an issue first to discuss what you'd like to change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Run tests (`pytest tests/ -v`)
4. Run linting (`ruff check .`)
5. Open a pull request

## License

[GPL-3.0](LICENSE)
