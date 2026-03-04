"""Regex-based PII scrubbing. Masks identifiers, preserves amounts/merchants/dates."""

from __future__ import annotations

import re

# Patterns that identify a person — account numbers, card numbers, cedulas, emails
_DATE_PATTERN = r"\b\d{1,4}[/\-\.]\d{1,2}[/\-\.]\d{2,4}\b"
_DECIMAL_PATTERN = r"\d+\.\d{1,2}"
_LONG_DIGITS = r"(?<!\d)(\d{8,})(?!\d)"  # 8+ digit sequences
_CARD_NUMBER = r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b"
_IBAN = r"\b[A-Z]{2}\d{2}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}\b"
_EMAIL = r"\b([A-Za-z0-9._%+\-]+)@([A-Za-z0-9.\-]+\.[A-Za-z]{2,})\b"


def redact_pii(text: str) -> str:
    """Mask identifiers in text. Preserves dates, decimal amounts, merchant names."""
    # Protect dates and decimals from digit masking
    protected: dict[str, str] = {}
    for i, m in enumerate(re.finditer(f"({_DATE_PATTERN})|({_DECIMAL_PATTERN})", text)):
        token = f"__PROTECTED_{i}__"
        protected[token] = m.group(0)
        text = text.replace(m.group(0), token, 1)

    # Mask identifiers
    text = re.sub(_CARD_NUMBER, lambda m: m.group(0)[:2] + "****" + m.group(0)[-2:], text)
    text = re.sub(_IBAN, "[IBAN-REDACTED]", text)
    text = re.sub(_LONG_DIGITS, lambda m: m.group(0)[:2] + "**" + m.group(0)[-2:], text)
    text = re.sub(_EMAIL, lambda m: m.group(1)[0] + "***@" + m.group(2), text)

    # Restore protected tokens
    for token, original in protected.items():
        text = text.replace(token, original)
    return text


def sanitize_error(error: Exception) -> str:
    """Strip potential PII from error messages before logging."""
    msg = str(error)
    msg = re.sub(r"'[^']{10,}'", "'[REDACTED]'", msg)
    return msg
