# How it works

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
