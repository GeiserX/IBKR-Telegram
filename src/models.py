"""Data models for trade signals and execution."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class TradeSignal:
    """A trade action (manual or programmatic)."""

    ticker: str
    action: str  # BUY, SELL, TRIM, ROLL
    target_weight_pct: float | None = None
    amount_description: str = ""
    related_ticker: str | None = None
    raw_text: str = ""
    source: str = "text"
    timestamp: datetime | None = None
    message_id: int | None = None
