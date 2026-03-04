"""Logfire observability with PII scrubbing."""

from __future__ import annotations

import logging
import os
from typing import Any

import logfire

from finance_query_agent.redaction import _CARD_NUMBER, _IBAN, _LONG_DIGITS, redact_pii

logger = logging.getLogger(__name__)


def scrubbing_callback(match: logfire.ScrubMatch) -> Any:
    """Apply PII redaction on string values in Logfire spans/logs."""
    if isinstance(match.value, str):
        return redact_pii(match.value)
    return match.value


def initialize() -> None:
    """Configure Logfire with PII scrubbing and Pydantic AI instrumentation."""
    if not os.environ.get("LOGFIRE_TOKEN"):
        logger.info("LOGFIRE_TOKEN not set, skipping Logfire initialization")
        return

    logfire.configure(
        scrubbing=logfire.ScrubbingOptions(
            extra_patterns=[_CARD_NUMBER, _IBAN, _LONG_DIGITS],
            callback=scrubbing_callback,
        ),
    )
    logfire.instrument_pydantic_ai()
