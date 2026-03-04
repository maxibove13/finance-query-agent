"""Tests for PII redaction — masks identifiers, preserves amounts/dates/merchants."""

from __future__ import annotations

from finance_query_agent.redaction import redact_pii, sanitize_error


class TestRedactPii:
    def test_masks_long_digit_sequences(self):
        result = redact_pii("account 0012345678 balance")
        assert "0012345678" not in result
        assert "00" in result  # first 2 preserved
        assert "78" in result  # last 2 preserved

    def test_masks_card_number(self):
        result = redact_pii("card 4532 1234 5678 9012 charged")
        assert "1234 5678" not in result
        assert "****" in result

    def test_masks_card_number_no_spaces(self):
        result = redact_pii("card 4532123456789012 charged")
        assert "45****12" in result

    def test_masks_iban(self):
        result = redact_pii("transfer to GB29 0012 0012 0012")
        assert "[IBAN-REDACTED]" in result

    def test_masks_email(self):
        result = redact_pii("email john.doe@example.com sent")
        assert "john.doe@example.com" not in result
        assert "j***@example.com" in result

    def test_preserves_dates(self):
        result = redact_pii("transaction on 2024-01-15 was processed")
        assert "2024-01-15" in result

    def test_preserves_decimal_amounts(self):
        result = redact_pii("spent 860.50 USD on groceries")
        assert "860.50" in result

    def test_preserves_merchant_names(self):
        result = redact_pii("purchase at Whole Foods Market for 125.99")
        assert "Whole Foods Market" in result
        assert "125.99" in result

    def test_mixed_content(self):
        text = "account 0012345678 spent 860.50 on 2024-01-15"
        result = redact_pii(text)
        assert "0012345678" not in result
        assert "860.50" in result
        assert "2024-01-15" in result

    def test_empty_string(self):
        assert redact_pii("") == ""

    def test_no_pii(self):
        text = "spent 500 USD on food"
        assert redact_pii(text) == text  # no 8+ digits, no cards, no emails


class TestSanitizeError:
    def test_strips_long_quoted_strings(self):
        err = Exception("failed to parse 'some very long description text here'")
        result = sanitize_error(err)
        assert "'[REDACTED]'" in result
        assert "some very long" not in result

    def test_preserves_short_quotes(self):
        err = Exception("column 'name' not found")
        result = sanitize_error(err)
        assert "'name'" in result
