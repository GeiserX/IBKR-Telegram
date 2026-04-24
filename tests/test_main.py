"""Tests for the entry-point main function."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AccountConfig, Config


@pytest.mark.asyncio
async def test_validation_errors_cause_exit():
    bad_cfg = Config()  # empty → validation errors
    with patch("src.__main__.load_config", return_value=bad_cfg), \
         pytest.raises(SystemExit) as exc_info:
        from src.__main__ import main
        await main()
    assert exc_info.value.code == 1


@pytest.mark.asyncio
async def test_successful_startup_calls_app_run():
    good_cfg = Config(
        bot_token="tok",
        admin_chat_id=1,
        accounts=[AccountConfig(name="a", gateway_host="h", gateway_port=5000)],
    )
    mock_app = MagicMock()
    mock_app.run = AsyncMock()

    with patch("src.__main__.load_config", return_value=good_cfg), \
         patch("src.app.App", return_value=mock_app) as mock_cls:
        from src.__main__ import main
        await main()

    mock_cls.assert_called_once_with(good_cfg)
    mock_app.run.assert_awaited_once()
