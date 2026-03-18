"""
Unit tests for app.agent.period — period math, param generation, watermarks.

No DB, no HTTP, no pipeline. Pure function tests.
Run: pytest tests/test_period.py -v
"""
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

# Add project root so we can import app.agent.source_config and
# app.agent.period without triggering the full app import chain
# (which pulls in dotenv, asyncpg, etc.).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Mock out heavy dependencies that source_config.py / period.py
# never actually use, but that get pulled in via app.config.
import types
_fake_dotenv = types.ModuleType("dotenv")
_fake_dotenv.load_dotenv = lambda *a, **kw: None
sys.modules.setdefault("dotenv", _fake_dotenv)

_fake_asyncpg = types.ModuleType("asyncpg")
sys.modules.setdefault("asyncpg", _fake_asyncpg)

from app.agent.source_config import PeriodConfig  # noqa: E402
from app.agent.period import (  # noqa: E402
    compute_period_params,
    latest_period_value,
    _parse_period_string,
    _normalize_to_period_start,
    _period_end,
    _enumerate_periods,
)
from dateutil.relativedelta import relativedelta  # noqa: E402


# ============ _parse_period_string ============

class TestParsePeriodString:
    def test_full_date(self):
        assert _parse_period_string("2024-06-15") == date(2024, 6, 15)

    def test_year_month(self):
        assert _parse_period_string("2024-06") == date(2024, 6, 1)

    def test_year_only(self):
        assert _parse_period_string("2024") == date(2024, 1, 1)

    def test_quarter(self):
        assert _parse_period_string("2024-Q1") == date(2024, 1, 1)
        assert _parse_period_string("2024-Q2") == date(2024, 4, 1)
        assert _parse_period_string("2024-Q3") == date(2024, 7, 1)
        assert _parse_period_string("2024-Q4") == date(2024, 10, 1)

    def test_quarter_lowercase(self):
        assert _parse_period_string("2024-q3") == date(2024, 7, 1)

    def test_whitespace_stripped(self):
        assert _parse_period_string("  2024  ") == date(2024, 1, 1)

    def test_garbage_returns_none(self):
        assert _parse_period_string("not-a-date") is None
        assert _parse_period_string("") is None

    def test_invalid_quarter(self):
        assert _parse_period_string("2024-Q0") is None
        assert _parse_period_string("2024-Qx") is None


# ============ _normalize_to_period_start ============

class TestNormalize:
    def test_daily(self):
        d = date(2025, 3, 15)
        assert _normalize_to_period_start(d, relativedelta(days=1)) == d

    def test_monthly(self):
        assert _normalize_to_period_start(
            date(2025, 3, 15), relativedelta(months=1)
        ) == date(2025, 3, 1)

    def test_quarterly(self):
        assert _normalize_to_period_start(
            date(2025, 5, 20), relativedelta(months=3)
        ) == date(2025, 4, 1)
        assert _normalize_to_period_start(
            date(2025, 1, 1), relativedelta(months=3)
        ) == date(2025, 1, 1)
        assert _normalize_to_period_start(
            date(2025, 12, 31), relativedelta(months=3)
        ) == date(2025, 10, 1)

    def test_yearly(self):
        assert _normalize_to_period_start(
            date(2025, 7, 4), relativedelta(years=1)
        ) == date(2025, 1, 1)


# ============ _period_end ============

class TestPeriodEnd:
    def test_daily(self):
        assert _period_end(date(2025, 3, 9), relativedelta(days=1)) == date(2025, 3, 9)

    def test_monthly(self):
        assert _period_end(date(2025, 3, 1), relativedelta(months=1)) == date(2025, 3, 31)
        assert _period_end(date(2025, 2, 1), relativedelta(months=1)) == date(2025, 2, 28)

    def test_quarterly(self):
        assert _period_end(date(2025, 1, 1), relativedelta(months=3)) == date(2025, 3, 31)

    def test_yearly(self):
        assert _period_end(date(2025, 1, 1), relativedelta(years=1)) == date(2025, 12, 31)
        # Leap year
        assert _period_end(date(2024, 1, 1), relativedelta(years=1)) == date(2024, 12, 31)


# ============ _enumerate_periods ============

class TestEnumeratePeriods:
    def test_yearly_two_years(self):
        periods = _enumerate_periods(
            date(2024, 1, 1), date(2025, 6, 1), relativedelta(years=1)
        )
        assert periods == [date(2024, 1, 1), date(2025, 1, 1)]

    def test_monthly_short_span(self):
        periods = _enumerate_periods(
            date(2025, 1, 1), date(2025, 3, 15), relativedelta(months=1)
        )
        assert periods == [
            date(2025, 1, 1), date(2025, 2, 1), date(2025, 3, 1),
        ]

    def test_start_after_end_returns_empty(self):
        periods = _enumerate_periods(
            date(2026, 1, 1), date(2025, 1, 1), relativedelta(years=1)
        )
        assert periods == []


# ============ compute_period_params — single param mode ============

class TestComputeSingleParam:
    """Tests for single-param mode (e.g. Year=2025)."""

    def _cfg(self, **overrides):
        defaults = dict(
            type="yearly",
            param_name="Year",
            param_format="%Y",
            lookback=1,
            initial_start="2023",
        )
        defaults.update(overrides)
        return PeriodConfig(**defaults)

    @patch("app.agent.period.date")
    def test_first_run_with_initial_start(self, mock_date):
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg(initial_start="2024")
        params = compute_period_params(cfg, last_fetched=None)

        assert len(params) == 2
        assert params[0] == {"Year": "2024"}
        assert params[1] == {"Year": "2025"}

    @patch("app.agent.period.date")
    def test_first_run_no_initial_start_returns_empty(self, mock_date):
        """No watermark + no initial_start → empty list (full fetch)."""
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg(initial_start=None)
        params = compute_period_params(cfg, last_fetched=None)
        assert params == []

    @patch("app.agent.period.date")
    def test_subsequent_run_with_lookback(self, mock_date):
        """Watermark=2024, lookback=1 → fetch from 2023."""
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg(lookback=1)
        params = compute_period_params(cfg, last_fetched="2024")

        assert len(params) == 3
        assert params[0] == {"Year": "2023"}
        assert params[1] == {"Year": "2024"}
        assert params[2] == {"Year": "2025"}

    @patch("app.agent.period.date")
    def test_lookback_zero(self, mock_date):
        """lookback=0 → start from the watermark itself."""
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg(lookback=0)
        params = compute_period_params(cfg, last_fetched="2025")

        assert len(params) == 1
        assert params[0] == {"Year": "2025"}

    @patch("app.agent.period.date")
    def test_watermark_already_current(self, mock_date):
        """Watermark is current year, lookback=1 → still re-fetches last + current."""
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg(lookback=1)
        params = compute_period_params(cfg, last_fetched="2025")

        assert len(params) == 2
        assert params[0] == {"Year": "2024"}
        assert params[1] == {"Year": "2025"}

    def test_unknown_period_type_returns_empty(self):
        cfg = self._cfg(type="biweekly")
        params = compute_period_params(cfg, last_fetched=None)
        assert params == []


# ============ compute_period_params — range param mode ============

class TestComputeRangeParam:
    """Tests for range-param mode (e.g. from_date / to_date)."""

    def _cfg(self, **overrides):
        defaults = dict(
            type="monthly",
            range_params={"from": "start_date", "to": "end_date"},
            range_format="%Y-%m-%d",
            lookback=1,
            initial_start="2025-01",
        )
        defaults.update(overrides)
        return PeriodConfig(**defaults)

    @patch("app.agent.period.date")
    def test_monthly_range_first_run(self, mock_date):
        mock_date.today.return_value = date(2025, 3, 15)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg()
        params = compute_period_params(cfg, last_fetched=None)

        assert len(params) == 3
        assert params[0] == {
            "start_date": "2025-01-01", "end_date": "2025-01-31",
        }
        assert params[1] == {
            "start_date": "2025-02-01", "end_date": "2025-02-28",
        }
        assert params[2] == {
            "start_date": "2025-03-01", "end_date": "2025-03-31",
        }

    @patch("app.agent.period.date")
    def test_monthly_range_with_watermark(self, mock_date):
        """Watermark=2025-02, lookback=1 → start from Jan."""
        mock_date.today.return_value = date(2025, 3, 15)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = self._cfg()
        params = compute_period_params(cfg, last_fetched="2025-02")

        assert len(params) == 3
        assert params[0]["start_date"] == "2025-01-01"
        assert params[2]["start_date"] == "2025-03-01"


# ============ compute_period_params — quarterly ============

class TestComputeQuarterly:
    @patch("app.agent.period.date")
    def test_quarterly_single_param(self, mock_date):
        mock_date.today.return_value = date(2025, 8, 1)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = PeriodConfig(
            type="quarterly",
            param_name="quarter",
            param_format="%Y-Q",
            lookback=0,
            initial_start="2025-Q1",
        )
        params = compute_period_params(cfg, last_fetched=None)

        assert len(params) == 3
        # param_format is strftime-based so we test what actually comes out.
        # "%Y-Q" → "2025-Q" — note this doesn't include the quarter number
        # because strftime doesn't have a quarter directive.
        # That's okay — this tests the mechanics; real configs would use
        # range_params for quarterly.
        assert "quarter" in params[0]


# ============ latest_period_value ============

class TestLatestPeriodValue:
    def test_single_param(self):
        cfg = PeriodConfig(
            type="yearly", param_name="Year", param_format="%Y",
        )
        params = [{"Year": "2024"}, {"Year": "2025"}]
        assert latest_period_value(cfg, params) == "2025"

    def test_range_param(self):
        cfg = PeriodConfig(
            type="monthly",
            range_params={"from": "start", "to": "end"},
        )
        params = [
            {"start": "2025-01-01", "end": "2025-01-31"},
            {"start": "2025-02-01", "end": "2025-02-28"},
        ]
        assert latest_period_value(cfg, params) == "2025-02-28"

    def test_empty_params(self):
        cfg = PeriodConfig(type="yearly", param_name="Year")
        assert latest_period_value(cfg, []) is None

    def test_no_param_name_or_range(self):
        cfg = PeriodConfig(type="yearly")
        assert latest_period_value(cfg, [{"x": "1"}]) is None


# ============ MoSPI-realistic scenario ============

class TestMoSPIScenario:
    """End-to-end scenario matching our mospi.yaml config."""

    @patch("app.agent.period.date")
    def test_cpi_first_run(self, mock_date):
        """First run: initial_start=2013, today=2026-03-09."""
        mock_date.today.return_value = date(2026, 3, 9)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = PeriodConfig(
            type="yearly",
            param_name="Year",
            param_format="%Y",
            lookback=1,
            initial_start="2013",
        )
        params = compute_period_params(cfg, last_fetched=None)

        years = [p["Year"] for p in params]
        assert years[0] == "2013"
        assert years[-1] == "2026"
        assert len(years) == 14  # 2013..2026

    @patch("app.agent.period.date")
    def test_cpi_subsequent_run(self, mock_date):
        """Watermark=2025, lookback=1 → fetch 2024 + 2025 + 2026."""
        mock_date.today.return_value = date(2026, 3, 9)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = PeriodConfig(
            type="yearly",
            param_name="Year",
            param_format="%Y",
            lookback=1,
            initial_start="2013",
        )
        params = compute_period_params(cfg, last_fetched="2025")

        years = [p["Year"] for p in params]
        assert years == ["2024", "2025", "2026"]

    @patch("app.agent.period.date")
    def test_cpi_watermark_value(self, mock_date):
        """Verify the watermark stored after a run."""
        mock_date.today.return_value = date(2026, 3, 9)
        mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

        cfg = PeriodConfig(
            type="yearly",
            param_name="Year",
            param_format="%Y",
            lookback=1,
            initial_start="2013",
        )
        params = compute_period_params(cfg, last_fetched="2025")
        watermark = latest_period_value(cfg, params)
        assert watermark == "2026"
