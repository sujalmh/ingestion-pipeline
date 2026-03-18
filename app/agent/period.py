"""
Period utilities — compute incremental time windows for period-aware fetching.

Given a PeriodConfig and an optional watermark (last fetched period), produces
the list of period parameter dicts that should be injected into API requests
so only new/recent data is fetched.

Pattern: Pure functions (no I/O), called by fetcher.py
"""
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta

from app.agent.source_config import PeriodConfig

logger = logging.getLogger(__name__)

_PERIOD_DELTAS = {
    "daily": relativedelta(days=1),
    "monthly": relativedelta(months=1),
    "quarterly": relativedelta(months=3),
    "yearly": relativedelta(years=1),
}


def compute_period_params(
    period_cfg: PeriodConfig,
    last_fetched: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Compute the list of parameter dicts to fetch, one per period window.

    For single-param mode each dict has {param_name: formatted_value}.
    For range-param mode each dict has {from_param: start, to_param: end}.

    Returns an empty list when no period filtering should be applied
    (e.g. first run with no initial_start configured — caller should do
    a full fetch).
    """
    today = date.today()
    delta = _PERIOD_DELTAS.get(period_cfg.type)
    if delta is None:
        logger.warning(
            f"Unknown period type '{period_cfg.type}', "
            "skipping period filtering"
        )
        return []

    start = _resolve_start(period_cfg, last_fetched, delta)
    if start is None:
        return []

    # Clamp start so we never request future periods
    if start > today:
        return []

    periods = _enumerate_periods(start, today, delta)
    if not periods:
        return []

    if period_cfg.range_params:
        return _build_range_params(periods, period_cfg, delta)
    elif period_cfg.param_name:
        return _build_single_params(periods, period_cfg)
    else:
        logger.warning(
            "PeriodConfig has neither param_name "
            "nor range_params, skipping"
        )
        return []


def latest_period_value(
    period_cfg: PeriodConfig,
    period_params: List[Dict[str, Any]],
) -> Optional[str]:
    """
    Extract the latest period string from the computed params to use as the
    new watermark value.  Returns a canonical YYYY-MM-DD string for storage.
    """
    if not period_params:
        return None

    last = period_params[-1]

    if period_cfg.range_params:
        to_key = period_cfg.range_params.get("to", "to_date")
        raw = str(last.get(to_key, ""))
    elif period_cfg.param_name:
        raw = str(last.get(period_cfg.param_name, ""))
    else:
        return None

    return raw if raw else None


# ============ Internal helpers ============


def _resolve_start(
    cfg: PeriodConfig,
    last_fetched: Optional[str],
    delta: relativedelta,
) -> Optional[date]:
    """Determine the start date for period enumeration."""
    if last_fetched:
        base = _parse_period_string(last_fetched)
        if base is None:
            logger.warning(f"Could not parse last_fetched_period '{last_fetched}'")
            return _initial_start_or_none(cfg)
        # Rewind by lookback periods from the watermark
        start = base
        for _ in range(cfg.lookback):
            start = start - delta
        return start

    return _initial_start_or_none(cfg)


def _initial_start_or_none(cfg: PeriodConfig) -> Optional[date]:
    """Parse initial_start from config, or return None (full-fetch signal)."""
    if cfg.initial_start:
        parsed = _parse_period_string(cfg.initial_start)
        if parsed:
            return parsed
        logger.warning(f"Could not parse initial_start '{cfg.initial_start}'")
    return None


def _parse_period_string(value: str) -> Optional[date]:
    """
    Best-effort parse of a period string into a date.
    Accepts: YYYY-MM-DD, YYYY-MM, YYYY, YYYY-Qn
    """
    from datetime import datetime

    value = value.strip()

    # YYYY-Qn (e.g. "2024-Q3")
    if "-Q" in value.upper():
        parts = value.upper().split("-Q")
        try:
            year = int(parts[0])
            quarter = int(parts[1])
            month = (quarter - 1) * 3 + 1
            return date(year, month, 1)
        except (ValueError, IndexError):
            return None

    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    return None


def _enumerate_periods(
    start: date, end: date, delta: relativedelta
) -> List[date]:
    """Generate a list of period start dates from start to end (inclusive)."""
    periods: List[date] = []
    current = _normalize_to_period_start(start, delta)
    while current <= end:
        periods.append(current)
        current = current + delta
    return periods


def _normalize_to_period_start(d: date, delta: relativedelta) -> date:
    """Snap a date to the start of its period."""
    if delta == relativedelta(days=1):
        return d
    if delta == relativedelta(months=1):
        return d.replace(day=1)
    if delta == relativedelta(months=3):
        quarter_month = ((d.month - 1) // 3) * 3 + 1
        return d.replace(month=quarter_month, day=1)
    if delta == relativedelta(years=1):
        return d.replace(month=1, day=1)
    return d


def _period_end(start: date, delta: relativedelta) -> date:
    """Compute the last day of a period starting at `start`."""
    return start + delta - timedelta(days=1)


def _build_single_params(
    periods: List[date], cfg: PeriodConfig
) -> List[Dict[str, Any]]:
    """Build one param dict per period for single-param mode."""
    fmt = cfg.param_format
    result = []
    for p in periods:
        result.append({cfg.param_name: p.strftime(fmt)})
    return result


def _build_range_params(
    periods: List[date], cfg: PeriodConfig, delta: relativedelta
) -> List[Dict[str, Any]]:
    """Build one param dict per period for range-param mode."""
    from_key = cfg.range_params.get("from", "from_date")
    to_key = cfg.range_params.get("to", "to_date")
    fmt = cfg.range_format or cfg.param_format
    result = []
    for p in periods:
        end = _period_end(p, delta)
        result.append({
            from_key: p.strftime(fmt),
            to_key: end.strftime(fmt),
        })
    return result
