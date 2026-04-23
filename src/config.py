"""Configuration loading and validation."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


def _int_env(name: str, fallback: int = 0) -> int:
    """Read an env var as int, returning fallback if empty or missing."""
    val = os.getenv(name, "")
    if not val:
        return fallback
    return int(val)


@dataclass
class AccountConfig:
    name: str
    gateway_host: str
    gateway_port: int
    max_allocation_pct: float = 100.0
    max_position_pct: float = 15.0
    margin_mode: str = "off"  # "soft" (sizing target), "hard" (auto-sell enforcement), "off"
    max_margin_usd: float = 0.0  # Margin cap in USD (0 = no limit, sizes to ~0% cash)
    is_margin_account: bool = False  # Auto-detected from IBKR on connect

    def __post_init__(self):
        # YAML parses bare `off` as False, `on` as True — normalize to string
        if isinstance(self.margin_mode, bool):
            self.margin_mode = "off" if not self.margin_mode else "soft"
        self.margin_mode = str(self.margin_mode).lower()
    display_name: str = ""
    net_deposits: float = 0.0
    flex_token: str = ""
    flex_query_id: int = 0


@dataclass
class TradingConfig:
    max_deviation_pct: float = 3.0
    order_type: str = "LMT"
    limit_offset_pct: float = 0.5
    confirm_before_execute: bool = True
    asset_types: list[str] = field(default_factory=lambda: ["options"])


@dataclass
class Config:
    # Telegram bot
    bot_token: str = ""
    admin_chat_id: int = 0

    # IBKR accounts
    accounts: list[AccountConfig] = field(default_factory=list)

    # Trading
    trading: TradingConfig = field(default_factory=TradingConfig)

    # Paths
    db_path: str = "data/trades.db"

    def validate(self) -> list[str]:
        """Validate essential config fields. Returns list of errors."""
        errors = []
        if not self.bot_token:
            errors.append("TELEGRAM_BOT_TOKEN is required")
        if self.bot_token and not self.admin_chat_id:
            errors.append(
                "TELEGRAM_ADMIN_CHAT_ID is required when bot_token is set "
                "(bot needs an admin to send confirmations to)"
            )
        if not self.accounts:
            errors.append("At least one IBKR account must be configured in config.yaml")
        for acc in self.accounts:
            if not acc.gateway_host:
                errors.append(f"Account '{acc.name}' is missing gateway_host")
            if not acc.gateway_port:
                errors.append(f"Account '{acc.name}' is missing gateway_port")
            if acc.margin_mode not in ("off", "soft", "hard"):
                errors.append(
                    f"Account '{acc.name}': margin_mode must be 'off', 'soft', or 'hard', "
                    f"got '{acc.margin_mode}'"
                )
            if acc.max_margin_usd < 0:
                errors.append(f"Account '{acc.name}': max_margin_usd cannot be negative")
        if self.trading.order_type not in ("LMT", "MKT"):
            errors.append(
                f"trading.order_type must be 'LMT' or 'MKT', "
                f"got '{self.trading.order_type}'"
            )
        return errors


def load_config() -> Config:
    """Load configuration from YAML file + environment variables."""
    config_path = os.getenv("CONFIG_PATH", "config.yaml")
    data = {}

    if Path(config_path).exists():
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}

    telegram = data.get("telegram", {})
    trading_data = data.get("trading", {})

    accounts = []
    for acc in data.get("accounts", []):
        accounts.append(AccountConfig(**acc))

    # Env var overrides for per-account margin: MARGIN_MODE_<NAME>, MAX_MARGIN_<NAME>
    for acc in accounts:
        mode_key = f"MARGIN_MODE_{acc.name.upper()}"
        mode_val = os.getenv(mode_key, "")
        if mode_val:
            acc.margin_mode = mode_val.lower()
        cap_key = f"MAX_MARGIN_{acc.name.upper()}"
        cap_val = os.getenv(cap_key, "")
        if cap_val:
            acc.max_margin_usd = float(cap_val)

    # Env vars override YAML
    return Config(
        bot_token=os.getenv("TELEGRAM_BOT_TOKEN", telegram.get("bot_token", "")),
        admin_chat_id=_int_env("TELEGRAM_ADMIN_CHAT_ID", telegram.get("admin_chat_id", 0)),
        accounts=accounts,
        trading=TradingConfig(**trading_data) if trading_data else TradingConfig(),
    )
