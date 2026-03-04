"""Tests for observability — scrubbing callback applies PII redaction correctly."""

from __future__ import annotations

import re

import logfire
import pytest

from finance_query_agent.observability import scrubbing_callback


def _make_match(value: object) -> logfire.ScrubMatch:
    """Create a ScrubMatch with the given value for testing."""
    # ScrubMatch requires path, value, pattern_match
    # We provide a dummy regex match for the pattern_match field
    dummy_re_match = re.search(".", "x")
    assert dummy_re_match is not None
    return logfire.ScrubMatch(path=("test",), value=value, pattern_match=dummy_re_match)


class TestScrubbingCallback:
    def test_redacts_card_number(self) -> None:
        match = _make_match("Payment to 4111 1111 1111 1111 done")
        result = scrubbing_callback(match)
        assert "4111 1111 1111 1111" not in result
        assert isinstance(result, str)

    def test_redacts_iban(self) -> None:
        match = _make_match("Transfer from DE89 3704 0044 0532")
        result = scrubbing_callback(match)
        assert "DE89" not in result
        assert "[IBAN-REDACTED]" in result

    def test_redacts_long_digits(self) -> None:
        match = _make_match("Account 12345678901234")
        result = scrubbing_callback(match)
        assert "12345678901234" not in result
        assert "**" in result

    def test_redacts_email(self) -> None:
        match = _make_match("Contact john.doe@example.com for details")
        result = scrubbing_callback(match)
        assert "john.doe@example.com" not in result
        assert "j***@example.com" in result

    def test_preserves_amounts(self) -> None:
        match = _make_match("Total: 1234.56")
        result = scrubbing_callback(match)
        assert "1234.56" in result

    def test_preserves_dates(self) -> None:
        match = _make_match("Date: 2024/01/15")
        result = scrubbing_callback(match)
        assert "2024/01/15" in result

    def test_passes_through_int(self) -> None:
        match = _make_match(42)
        result = scrubbing_callback(match)
        assert result == 42

    def test_passes_through_float(self) -> None:
        match = _make_match(3.14)
        result = scrubbing_callback(match)
        assert result == 3.14

    def test_passes_through_none(self) -> None:
        match = _make_match(None)
        result = scrubbing_callback(match)
        assert result is None

    def test_passes_through_dict(self) -> None:
        d = {"key": "value"}
        match = _make_match(d)
        result = scrubbing_callback(match)
        assert result is d

    def test_passes_through_list(self) -> None:
        lst = [1, 2, 3]
        match = _make_match(lst)
        result = scrubbing_callback(match)
        assert result is lst

    def test_clean_string_unchanged(self) -> None:
        match = _make_match("Netflix subscription payment")
        result = scrubbing_callback(match)
        assert result == "Netflix subscription payment"


class TestInitialize:
    def test_initialize_skips_without_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """initialize() should not crash when LOGFIRE_TOKEN is not set."""
        monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
        from finance_query_agent.observability import initialize

        # Should not raise
        initialize()

    def test_initialize_calls_configure_with_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOGFIRE_TOKEN", "test-token")
        from unittest.mock import patch

        with patch("finance_query_agent.observability.logfire") as mock_logfire:
            from finance_query_agent.observability import initialize

            initialize()
            mock_logfire.configure.assert_called_once()
            mock_logfire.instrument_pydantic_ai.assert_called_once()
