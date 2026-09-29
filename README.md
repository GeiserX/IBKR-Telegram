<p align="center">
  <img src="https://raw.githubusercontent.com/GeiserX/IBKR-Telegram/main/docs/images/banner.svg" alt="IBKR-Telegram" width="100%">
</p>

<h1 align="center">IBKR-Telegram</h1>

<p align="center">
  <a href="https://hub.docker.com/r/drumsergio/ibkr-telegram"><img src="https://img.shields.io/docker/v/drumsergio/ibkr-telegram?style=flat-square&logo=docker&logoColor=white&label=Docker&sort=semver" alt="Docker"></a>
  <a href="https://github.com/GeiserX/IBKR-Telegram/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/GeiserX/IBKR-Telegram/ci.yml?style=flat-square&logo=github&label=CI" alt="CI"></a>
  <a href="https://github.com/GeiserX/IBKR-Telegram/blob/main/LICENSE"><img src="https://img.shields.io/github/license/GeiserX/IBKR-Telegram?style=flat-square" alt="License"></a>
  <a href="https://hub.docker.com/r/drumsergio/ibkr-telegram"><img src="https://img.shields.io/docker/pulls/drumsergio/ibkr-telegram?style=flat-square&logo=docker&logoColor=white" alt="Docker Pulls"></a>
  <a href="https://app.codecov.io/gh/GeiserX/IBKR-Telegram"><img src="https://img.shields.io/codecov/c/github/GeiserX/IBKR-Telegram?style=flat-square&logo=codecov&logoColor=white" alt="Codecov"></a>
</p>

<p align="center">Self-hosted Telegram bot for Interactive Brokers, run with Docker Compose next to IB Gateway. Manage your portfolio, execute trades and monitor positions from Telegram.</p>

## Disclaimer

This software is provided "as is" under the [GPL-3.0 license](https://github.com/GeiserX/IBKR-Telegram/blob/main/LICENSE). **Use at your own risk.** The author(s) accept no liability for financial losses. This is not financial advice — test thoroughly with paper trading accounts before using real money.

## Features

- Several IBKR accounts from one bot, each on its own IB Gateway container, with per-account limits and margin modes.
- Any instrument IBKR supports (stocks, options, futures), chosen with `asset_types` in `config.yaml`.
- An option chain wizard for LEAPS: deep ITM strike matching and automatic contract resolution.
- Checks before every trade: market hours, duplicate signals, position limits, and a Telegram confirmation.
- Periodic position snapshots, P&L tracking, and deposit/withdrawal history through the IBKR Flex Web Service.
- Soft (alert) or hard (auto-sell) margin enforcement per account.
- An optional webhook API for signals from TradingView or your own parsers, behind the same confirmation step.

## Quick start

You need an IBKR account with API access, a bot token from [@BotFather](https://t.me/BotFather), and Docker Compose.

```bash
git clone https://github.com/GeiserX/IBKR-Telegram.git && cd IBKR-Telegram
cp .env.example .env && cp config.example.yaml config.yaml && cp docker-compose.example.yml docker-compose.yml
docker compose up -d
```

Fill in `.env` and `config.yaml` before starting, complete the IB Gateway 2FA on first start, then send `/status` to the bot. [Getting started](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/getting-started.md) has every step.

## Documentation

- [Getting started](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/getting-started.md): prerequisites and first start
- [Configuration](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/configuration.md): `config.yaml`, environment variables, margin modes, IB Gateway
- [Usage](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/usage.md): every Telegram command
- [Webhook API](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/webhook.md): sending trade signals over HTTP
- [How it works](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/how-it-works.md): how the pieces fit
- [Troubleshooting](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/troubleshooting.md)
- [Development](https://github.com/GeiserX/IBKR-Telegram/blob/main/docs/development.md): tests, linting and contributing

## License

[GPL-3.0-or-later](https://github.com/GeiserX/IBKR-Telegram/blob/main/LICENSE)
