# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |
| < latest | :x:               |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do NOT open a public issue.**
2. Use [GitHub Security Advisories](https://github.com/GeiserX/IBKR-Telegram/security/advisories/new) to privately report the vulnerability.
3. Alternatively, email: `9169332+GeiserX@users.noreply.github.com`

**Response timeline:**
- Acknowledgment: within 72 hours
- Assessment: within 1 week
- Fix: as soon as reasonably possible

## Security Architecture

### Credential Management
- All credentials (IBKR accounts, bot tokens) are stored in environment variables or `.env` files, never in code.
- `.env` and `config.yaml` are gitignored.

### IBKR Gateway
- IB Gateway ports are bound to `127.0.0.1` only (not exposed to network).
- Each gateway container runs with its own credentials.
- VNC ports (for debugging) are also localhost-only.

### Trade Execution
- Every trade requires explicit user confirmation via Telegram bot before execution.
- Position limits per-account to prevent over-allocation.
- Margin compliance checks (soft alerts or hard auto-sell enforcement).

### Container Security
- Application container runs as non-root user.
- Read-only filesystem with tmpfs for temporary files.
- All capabilities dropped (`cap_drop: ALL`).
- No privilege escalation (`no-new-privileges`).

## Self-Hosting Recommendations

1. Run behind a firewall — no ports need to be exposed to the internet.
2. Use strong, unique passwords for IBKR accounts.
3. Enable 2FA on all IBKR accounts.
4. Regularly rotate the Telegram bot token.
5. Monitor the trade execution log for unexpected activity.
