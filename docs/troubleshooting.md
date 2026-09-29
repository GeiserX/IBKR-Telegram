# Troubleshooting

## The bot does not answer

Cause: the bot only obeys the Telegram user in `TELEGRAM_ADMIN_CHAT_ID`. Commands from anyone else get no reply, and their taps on inline buttons get an "Unauthorized" alert. When the variable is missing or `0`, nobody is the admin.

Fix: set `TELEGRAM_ADMIN_CHAT_ID` in `.env` to your numeric Telegram user id and recreate the container.

## `/status` shows an account as disconnected

Cause: the bot cannot reach that account's IB Gateway, or the gateway is not logged in. On the first start the gateway waits for your two-factor approval.

Fix: read `docker logs ib-gateway` and approve the login in the IBKR app. Check that the gateway host and port for the account in `config.yaml` match the gateway container. See [Configuration](configuration.md#ib-gateway).

## A confirmed trade answers "Market closed — next open in ..."

Cause: trades run only during the regular US session, 9:30 to 16:00 Eastern time on weekdays. The signal is marked skipped. Market holidays are not checked; IBKR rejects those orders itself.

Fix: send the signal again when the market is open.

## A signal answers "Duplicate signal: ... already processed recently"

Cause: the same action on the same ticker was already processed in the last 4 hours. A webhook signal gets `"status": "duplicate_skipped"` instead.

Fix: none needed if it is a repeat. Wait for the window to pass to send the same trade again.

## A buy skips one of the accounts

Cause: the trade would break that account's `max_position_pct` or `max_allocation_pct` in `config.yaml`. The log names the account and the limit (`Position limit: ...`). Sells are never blocked.

Fix: raise the limit for that account, or trade a smaller percentage.

## Reporting a bug

Open an [issue](https://github.com/GeiserX/IBKR-Telegram/issues) with:

- the image tag you run;
- the log around the problem (`docker logs ibkr-telegram`);
- the command or signal you sent and what the bot answered.

Leave out account numbers, credentials, tokens and position sizes.
