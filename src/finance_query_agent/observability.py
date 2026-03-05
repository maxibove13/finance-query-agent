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


def initialize() -> bool:
    """Configure Logfire with PII scrubbing and Pydantic AI instrumentation.

    Returns True on success (or when skipped due to missing token), False on failure.
    """
    if not os.environ.get("LOGFIRE_TOKEN"):
        logger.info("LOGFIRE_TOKEN not set, skipping Logfire initialization")
        return True

    try:
        logfire.configure(
            scrubbing=logfire.ScrubbingOptions(
                extra_patterns=[_CARD_NUMBER, _IBAN, _LONG_DIGITS],
                callback=scrubbing_callback,
            ),
        )
        logfire.instrument_pydantic_ai()
    except Exception:
        logger.error("Logfire initialization failed", exc_info=True)
        return False
    return True
