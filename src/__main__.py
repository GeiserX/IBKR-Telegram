"""Entry point for the IBKR-Telegram bot."""

import asyncio
import logging
import sys

from .config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ibkr_telegram")


async def main() -> None:
    config = load_config()

    errors = config.validate()
    if errors:
        for err in errors:
            logger.error(f"Config error: {err}")
        sys.exit(1)

    logger.info("Starting IBKR-Telegram...")

    from .app import App

    app = App(config)
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
