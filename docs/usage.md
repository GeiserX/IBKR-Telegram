# Usage

## Commands

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

## What the bot does

- **Multi-account trading** — manage multiple IBKR accounts simultaneously from a single bot instance. Each account connects to its own IB Gateway container. The same percentage allocation is applied proportionally across all accounts, with per-account settings for position limits, margin modes, and display names — all configured in `config.yaml`.
- **Configurable instrument support** — trade any instrument supported by IBKR, including stocks, options, futures, and more. The `asset_types` field in config controls which instruments are active. A built-in option chain wizard provides LEAPS selection with deep ITM strike matching and automatic contract resolution.
- **Safety-first execution** — market hours checks, duplicate signal detection, position limit enforcement, and mandatory confirmation before every trade.
- **Real-time portfolio sync** — periodic position snapshots, P&L tracking, and Flex Web Service integration for deposit/withdrawal history.
- **Margin compliance** — configurable soft (alert) or hard (auto-sell) margin enforcement per account.
- **Webhook API** — optional HTTP endpoint for receiving trade signals from external sources (TradingView, custom parsers, etc.). See [Webhook API](webhook.md).
