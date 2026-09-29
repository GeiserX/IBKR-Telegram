# Installation

## Prerequisites

- An [Interactive Brokers](https://www.interactivebrokers.com/) account with API access enabled
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- Docker and Docker Compose

## Steps

1. **Copy the example files:**

   ```bash
   cp .env.example .env
   cp config.example.yaml config.yaml
   cp docker-compose.example.yml docker-compose.yml
   ```

2. **Edit `.env`** with your Telegram bot token, admin chat ID, and IBKR credentials.

3. **Edit `config.yaml`** with your account names, gateway hosts, and trading preferences. See [Configuration](configuration.md).

4. **Start the stack:**

   ```bash
   docker compose up -d
   ```

5. **Complete IB Gateway 2FA** — on first start, the gateway container will wait for your two-factor authentication. Check container logs for instructions.

6. **Send `/status`** in Telegram to verify connectivity.
