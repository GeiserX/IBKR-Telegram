"""Tests for configuration loading and validation."""

import yaml

from src.config import AccountConfig, Config, TradingConfig, _int_env, load_config

# ---------------------------------------------------------------------------
# _int_env
# ---------------------------------------------------------------------------

class TestIntEnv:
    def test_missing_env_returns_fallback(self, monkeypatch):
        monkeypatch.delenv("TEST_INT_ENV_MISSING", raising=False)
        assert _int_env("TEST_INT_ENV_MISSING", 42) == 42

    def test_empty_env_returns_fallback(self, monkeypatch):
        monkeypatch.setenv("TEST_INT_ENV_EMPTY", "")
        assert _int_env("TEST_INT_ENV_EMPTY", 7) == 7

    def test_valid_int_env(self, monkeypatch):
        monkeypatch.setenv("TEST_INT_ENV_VALID", "123")
        assert _int_env("TEST_INT_ENV_VALID", 0) == 123


# ---------------------------------------------------------------------------
# AccountConfig.__post_init__
# ---------------------------------------------------------------------------

class TestAccountConfigPostInit:
    def test_false_becomes_off(self):
        acc = AccountConfig(name="a", gateway_host="h", gateway_port=1, margin_mode=False)
        assert acc.margin_mode == "off"

    def test_true_becomes_soft(self):
        acc = AccountConfig(name="a", gateway_host="h", gateway_port=1, margin_mode=True)
        assert acc.margin_mode == "soft"

    def test_string_hard_stays_hard(self):
        acc = AccountConfig(name="a", gateway_host="h", gateway_port=1, margin_mode="hard")
        assert acc.margin_mode == "hard"

    def test_string_uppercased_is_lowered(self):
        acc = AccountConfig(name="a", gateway_host="h", gateway_port=1, margin_mode="SOFT")
        assert acc.margin_mode == "soft"


# ---------------------------------------------------------------------------
# Config.validate
# ---------------------------------------------------------------------------

class TestConfigValidate:
    def test_empty_config_errors(self):
        errors = Config().validate()
        assert any("TELEGRAM_BOT_TOKEN" in e for e in errors)
        assert any("account" in e.lower() for e in errors)

    def test_bot_token_without_admin_chat_id(self):
        cfg = Config(bot_token="tok", admin_chat_id=0)
        errors = cfg.validate()
        assert any("TELEGRAM_ADMIN_CHAT_ID" in e for e in errors)

    def test_bot_token_with_admin_but_no_accounts(self):
        cfg = Config(bot_token="tok", admin_chat_id=1)
        errors = cfg.validate()
        assert any("account" in e.lower() for e in errors)
        assert not any("TELEGRAM_ADMIN_CHAT_ID" in e for e in errors)

    def test_account_missing_gateway_host(self):
        acc = AccountConfig(name="x", gateway_host="", gateway_port=5000)
        cfg = Config(bot_token="tok", admin_chat_id=1, accounts=[acc])
        errors = cfg.validate()
        assert any("gateway_host" in e for e in errors)

    def test_account_missing_gateway_port(self):
        acc = AccountConfig(name="x", gateway_host="h", gateway_port=0)
        cfg = Config(bot_token="tok", admin_chat_id=1, accounts=[acc])
        errors = cfg.validate()
        assert any("gateway_port" in e for e in errors)

    def test_account_invalid_margin_mode(self):
        acc = AccountConfig(name="x", gateway_host="h", gateway_port=5000, margin_mode="bad")
        cfg = Config(bot_token="tok", admin_chat_id=1, accounts=[acc])
        errors = cfg.validate()
        assert any("margin_mode" in e for e in errors)

    def test_account_negative_max_margin(self):
        acc = AccountConfig(name="x", gateway_host="h", gateway_port=5000, max_margin_usd=-1)
        cfg = Config(bot_token="tok", admin_chat_id=1, accounts=[acc])
        errors = cfg.validate()
        assert any("max_margin_usd" in e for e in errors)

    def test_invalid_order_type(self):
        trading = TradingConfig(order_type="FOK")
        acc = AccountConfig(name="x", gateway_host="h", gateway_port=5000)
        cfg = Config(bot_token="tok", admin_chat_id=1, accounts=[acc], trading=trading)
        errors = cfg.validate()
        assert any("order_type" in e for e in errors)

    def test_valid_complete_config(self):
        acc = AccountConfig(name="live", gateway_host="localhost", gateway_port=5000)
        cfg = Config(bot_token="tok", admin_chat_id=1, accounts=[acc])
        assert cfg.validate() == []

    def test_webhook_only_still_requires_bot_and_accounts(self):
        cfg = Config(webhook_secret="sec")
        errors = cfg.validate()
        assert any("TELEGRAM_BOT_TOKEN" in e for e in errors)
        assert any("account" in e.lower() for e in errors)


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_no_yaml_no_env_returns_defaults(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CONFIG_PATH", str(tmp_path / "nope.yaml"))
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
        monkeypatch.delenv("WEBHOOK_PORT", raising=False)
        cfg = load_config()
        assert cfg.bot_token == ""
        assert cfg.admin_chat_id == 0
        assert cfg.accounts == []
        assert cfg.webhook_port == 8080

    def test_yaml_with_accounts_and_trading(self, tmp_path, monkeypatch):
        data = {
            "telegram": {"bot_token": "yaml-tok", "admin_chat_id": 99},
            "accounts": [
                {"name": "ib1", "gateway_host": "gw", "gateway_port": 4001}
            ],
            "trading": {"order_type": "MKT", "max_deviation_pct": 5.0},
        }
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump(data))
        monkeypatch.setenv("CONFIG_PATH", str(cfg_file))
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
        monkeypatch.delenv("WEBHOOK_PORT", raising=False)
        cfg = load_config()
        assert cfg.bot_token == "yaml-tok"
        assert cfg.admin_chat_id == 99
        assert len(cfg.accounts) == 1
        assert cfg.accounts[0].name == "ib1"
        assert cfg.trading.order_type == "MKT"
        assert cfg.trading.max_deviation_pct == 5.0

    def test_env_vars_override_yaml(self, tmp_path, monkeypatch):
        data = {
            "telegram": {"bot_token": "yaml-tok", "admin_chat_id": 1},
            "webhook_secret": "yaml-sec",
            "webhook_port": 9090,
        }
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump(data))
        monkeypatch.setenv("CONFIG_PATH", str(cfg_file))
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "env-tok")
        monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "55")
        monkeypatch.setenv("WEBHOOK_SECRET", "env-sec")
        monkeypatch.setenv("WEBHOOK_PORT", "7070")
        cfg = load_config()
        assert cfg.bot_token == "env-tok"
        assert cfg.admin_chat_id == 55
        assert cfg.webhook_secret == "env-sec"
        assert cfg.webhook_port == 7070

    def test_per_account_env_overrides(self, tmp_path, monkeypatch):
        data = {
            "accounts": [
                {
                    "name": "live",
                    "gateway_host": "gw",
                    "gateway_port": 4001,
                    "margin_mode": "off",
                    "max_margin_usd": 0,
                }
            ],
        }
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump(data))
        monkeypatch.setenv("CONFIG_PATH", str(cfg_file))
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
        monkeypatch.delenv("WEBHOOK_PORT", raising=False)
        monkeypatch.setenv("MARGIN_MODE_LIVE", "hard")
        monkeypatch.setenv("MAX_MARGIN_LIVE", "5000")
        cfg = load_config()
        assert cfg.accounts[0].margin_mode == "hard"
        assert cfg.accounts[0].max_margin_usd == 5000.0

    def test_config_path_env_var(self, tmp_path, monkeypatch):
        data = {"telegram": {"bot_token": "custom-path"}}
        custom = tmp_path / "custom.yaml"
        custom.write_text(yaml.dump(data))
        monkeypatch.setenv("CONFIG_PATH", str(custom))
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
        monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
        monkeypatch.delenv("WEBHOOK_PORT", raising=False)
        cfg = load_config()
        assert cfg.bot_token == "custom-path"
