"""
Data Fetcher — HTTP fetcher with retry + rate limiting.

Fetches data from configured API endpoints with automatic auth header
injection, retry with exponential backoff, and custom header support.
Supports period-aware incremental fetching when an API has a PeriodConfig.

Pattern: Module-level async functions + @dataclass (adapter-style, like vector_adapter.py)
"""
import logging
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from app.agent.auth_manager import get_auth_headers, AuthError
from app.agent.source_config import SourceConfig, APIEndpointConfig
from app.config import settings

logger = logging.getLogger(__name__)

# Populated by period-aware fetch; orchestrator reads this after a run
# to persist the watermark.  Keyed by (source_id, api_id).
_period_watermarks: Dict[tuple, str] = {}


# ============ Data Classes ============

@dataclass
class FetchResult:
    """Result from fetching a single API endpoint."""
    success: bool
    source_id: str
    api_id: str
    data: Optional[Any] = None
    content_type: Optional[str] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    row_count: Optional[int] = None


# ============ Public Functions ============

async def fetch_api(source_config: SourceConfig, api_config: APIEndpointConfig) -> FetchResult:
    """
    Fetch data from a single API endpoint with optional auto-pagination.

    If the endpoint has a PeriodConfig, the fetcher looks up the last-fetched
    watermark and only requests data for new/recent periods.  Results from all
    period windows are merged into a single FetchResult.
    """
    sid = source_config.source_id
    aid = api_config.id

    # --- Period-aware path ---
    if api_config.period:
        return await _fetch_with_periods(source_config, api_config)

    # --- Standard (full-fetch) path ---
    return await _fetch_full(source_config, api_config)


async def _fetch_full(source_config: SourceConfig, api_config: APIEndpointConfig) -> FetchResult:
    """Original full-fetch logic (no period filtering)."""
    url = source_config.base_url.rstrip("/") + api_config.endpoint
    sid = source_config.source_id
    aid = api_config.id

    try:
        headers = await get_auth_headers(source_config)
    except AuthError as e:
        return FetchResult(success=False, source_id=sid, api_id=aid, error=str(e))
    if api_config.headers:
        headers.update(api_config.headers)

    pagination = api_config.pagination or {}
    pag_type = pagination.get("type", "none")

    if pag_type == "none" or api_config.output_format != "json":
        res = await _fetch_single_page(url, api_config.method, headers, api_config.params or {}, api_config.output_format, sid, aid)
        if res.success:
            if _is_empty_response(res.data):
                logger.warning(f"{sid}/{aid}: API returned empty data")
                return FetchResult(
                    success=False, source_id=sid, api_id=aid,
                    error="API returned empty data (no records)",
                    status_code=res.status_code,
                )
            logger.info(f"Fetched {sid}/{aid}: status={res.status_code}, rows={res.row_count}")
        else:
            logger.warning(f"Error fetching {sid}/{aid}: {res.error}")
        return res

    return await _fetch_paginated(url, sid, aid, api_config, headers, pagination)


async def _fetch_paginated(
    url: str, sid: str, aid: str,
    api_config: APIEndpointConfig, headers: dict, pagination: dict,
) -> FetchResult:
    """Paginated fetch (shared by full and period paths)."""
    pag_type = pagination.get("type", "none")
    all_data = []
    max_pages = int(pagination.get("max_pages", 50))
    page_param = pagination.get("page_param", "page")
    offset_param = pagination.get("offset_param", "offset")
    size_param = pagination.get("size_param", "limit")
    size = int(pagination.get("size", 100))

    current_page = 1
    current_offset = 0
    last_content_type = None
    total_rows = 0

    while current_page <= max_pages:
        page_params = dict(api_config.params or {})
        if pag_type == "page":
            page_params[page_param] = current_page
            page_params[size_param] = size
        elif pag_type == "offset":
            page_params[offset_param] = current_offset
            page_params[size_param] = size

        logger.info(f"Fetching {sid}/{aid} (Page {current_page}/{max_pages})")
        res = await _fetch_single_page(url, api_config.method, headers, page_params, api_config.output_format, sid, aid)

        if not res.success:
            if current_page == 1:
                return res
            logger.warning(f"Pagination stopped early at page {current_page} due to error: {res.error}")
            break

        last_content_type = res.content_type

        page_records = _normalize_records(res.data)

        if not page_records:
            break

        all_data.extend(page_records)
        total_rows += len(page_records)

        if len(page_records) < size:
            break

        current_page += 1
        current_offset += size

    return FetchResult(
        success=True,
        source_id=sid,
        api_id=aid,
        data=all_data,
        content_type=last_content_type,
        status_code=200,
        row_count=total_rows,
    )


async def _fetch_with_periods(
    source_config: SourceConfig, api_config: APIEndpointConfig,
) -> FetchResult:
    """
    Period-aware fetch: look up watermark, compute delta periods, fetch each
    window, merge results, and record the new watermark for the orchestrator.
    """
    from app.agent.period import compute_period_params, latest_period_value
    from app.models.db import get_last_fetched_period

    sid = source_config.source_id
    aid = api_config.id
    period_cfg = api_config.period

    last_fetched = await get_last_fetched_period(sid, aid)
    period_params_list = compute_period_params(period_cfg, last_fetched)

    if not period_params_list:
        if last_fetched:
            logger.info(f"{sid}/{aid}: no new periods since watermark '{last_fetched}', doing full fetch as safety")
        else:
            logger.info(f"{sid}/{aid}: first run with no initial_start, doing full fetch")
        result = await _fetch_full(source_config, api_config)
        return result

    logger.info(
        f"{sid}/{aid}: period-aware fetch — {len(period_params_list)} period(s) "
        f"(watermark={last_fetched})"
    )

    url = source_config.base_url.rstrip("/") + api_config.endpoint
    try:
        headers = await get_auth_headers(source_config)
    except AuthError as e:
        return FetchResult(success=False, source_id=sid, api_id=aid, error=str(e))
    if api_config.headers:
        headers.update(api_config.headers)

    pagination = api_config.pagination or {}
    pag_type = pagination.get("type", "none")

    all_data: list = []
    last_content_type = None
    total_rows = 0
    errors: List[str] = []

    for i, p_params in enumerate(period_params_list, 1):
        merged_params = dict(api_config.params or {})
        merged_params.update(p_params)

        logger.info(f"{sid}/{aid}: fetching period {i}/{len(period_params_list)} — {p_params}")

        if pag_type != "none" and api_config.output_format == "json":
            # Clone api_config with merged params for the paginated helper
            period_api = api_config.model_copy(update={"params": merged_params})
            res = await _fetch_paginated(url, sid, aid, period_api, headers, pagination)
        else:
            res = await _fetch_single_page(
                url, api_config.method, headers, merged_params,
                api_config.output_format, sid, aid,
            )

        if not res.success:
            errors.append(f"Period {p_params}: {res.error}")
            continue

        last_content_type = res.content_type

        records = _normalize_records(res.data)

        if records:
            all_data.extend(records)
            total_rows += len(records)

    if not all_data and errors:
        return FetchResult(
            success=False, source_id=sid, api_id=aid,
            error=f"All period fetches failed: {'; '.join(errors)}",
        )

    # Store watermark for the orchestrator to persist after pipeline feed
    new_watermark = latest_period_value(period_cfg, period_params_list)
    if new_watermark:
        _period_watermarks[(sid, aid)] = new_watermark

    if errors:
        logger.warning(f"{sid}/{aid}: {len(errors)} period(s) failed: {'; '.join(errors)}")

    return FetchResult(
        success=True,
        source_id=sid,
        api_id=aid,
        data=all_data,
        content_type=last_content_type,
        status_code=200,
        row_count=total_rows,
    )


def pop_period_watermark(source_id: str, api_id: str) -> Optional[str]:
    """Pop and return the watermark recorded during the last period-aware fetch."""
    return _period_watermarks.pop((source_id, api_id), None)


async def _fetch_single_page(url: str, method: str, headers: dict, params: dict, output_format: str, sid: str, aid: str) -> FetchResult:
    """Helper to perform a single HTTP request with retries."""
    max_retries = settings.AGENT_RETRY_ATTEMPTS
    timeout = httpx.Timeout(float(settings.AGENT_FETCH_TIMEOUT))

    _SUCCESS_CODES = {200, 201, 202}
    _RETRYABLE_CODES = {429, 500, 502, 503, 504}

    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                if method.upper() == "POST":
                    post_headers = {**headers, "Content-Type": "application/json"}
                    resp = await client.post(url, headers=post_headers, json=params)
                else:
                    resp = await client.get(url, headers=headers, params=params)

            content_type = resp.headers.get("content-type", "")

            if resp.status_code in _SUCCESS_CODES:
                if resp.status_code == 204 or not resp.content:
                    return FetchResult(
                        success=True, source_id=sid, api_id=aid,
                        data=[], content_type=content_type,
                        status_code=resp.status_code, row_count=0,
                    )

                if "json" in content_type or output_format == "json":
                    try:
                        data = resp.json()
                    except Exception:
                        data = resp.text
                else:
                    data = resp.text

                return FetchResult(
                    success=True,
                    source_id=sid,
                    api_id=aid,
                    data=data,
                    content_type=content_type,
                    status_code=resp.status_code,
                    row_count=_estimate_row_count(data),
                )

            if resp.status_code in _RETRYABLE_CODES and attempt < max_retries:
                wait = 2 ** attempt
                logger.info(f"{sid}/{aid}: HTTP {resp.status_code}, retrying in {wait}s (attempt {attempt}/{max_retries})")
                await asyncio.sleep(wait)
                continue

            error_body = _safe_error_text(resp)
            return FetchResult(
                success=False, source_id=sid, api_id=aid,
                error=f"HTTP {resp.status_code}: {error_body}",
                status_code=resp.status_code,
            )

        except httpx.TimeoutException:
            if attempt < max_retries:
                wait = 2 ** attempt
                logger.info(f"{sid}/{aid}: timeout, retrying in {wait}s (attempt {attempt}/{max_retries})")
                await asyncio.sleep(wait)
                continue
            return FetchResult(success=False, source_id=sid, api_id=aid, error=f"Timeout after {settings.AGENT_FETCH_TIMEOUT}s")

        except Exception as e:
            if attempt < max_retries:
                wait = 2 ** attempt
                await asyncio.sleep(wait)
                continue
            return FetchResult(success=False, source_id=sid, api_id=aid, error=f"Request failed: {str(e)}")

    return FetchResult(success=False, source_id=sid, api_id=aid, error="Exhausted all retry attempts")


async def fetch_all_apis(source_config: SourceConfig) -> List[FetchResult]:
    """
    Fetch data from all enabled APIs in a source config (sequentially).

    Args:
        source_config: The source configuration.

    Returns:
        List of FetchResult for each API endpoint.
    """
    results: List[FetchResult] = []

    for api_config in source_config.apis:
        if not api_config.enabled:
            logger.info(f"Skipping disabled API: {source_config.source_id}/{api_config.id}")
            continue

        result = await fetch_api(source_config, api_config)
        results.append(result)

    return results


async def test_fetch_api(
    base_url: str,
    endpoint: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    auth_type: str = "none",
    auth_login_url: Optional[str] = None,
    auth_credentials: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Test-fetch an API endpoint without saving. Used to validate new sources.

    Args:
        base_url: Base URL of the API.
        endpoint: API endpoint path.
        method: HTTP method (GET/POST).
        params: Query parameters or JSON body.
        headers: Custom headers.
        auth_type: Auth type (token/api_key/none).
        auth_login_url: Login URL for token auth.
        auth_credentials: Credentials dict.

    Returns:
        Dict with success, status_code, content_type, row_count, preview, error.
    """
    url = base_url.rstrip("/") + endpoint
    req_headers = dict(headers or {})

    from app.agent.auth_manager import _extract_token

    if auth_type == "api_key" and auth_credentials:
        api_key = (auth_credentials.get("api_key")
                   or auth_credentials.get("key")
                   or auth_credentials.get("apikey", ""))
        if api_key:
            header_name = auth_credentials.get("header_name", "Authorization")
            prefix = auth_credentials.get("header_prefix", "Bearer ")
            req_headers[header_name] = f"{prefix}{api_key}"

    elif auth_type == "token" and auth_login_url and auth_credentials:
        login_url = base_url.rstrip("/") + auth_login_url
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                login_resp = await client.post(
                    login_url, json=auth_credentials,
                    headers={"Content-Type": "application/json"},
                )
            if login_resp.status_code in (200, 201):
                login_data = login_resp.json()
                token = _extract_token(login_data)
                if token:
                    req_headers["Authorization"] = f"Bearer {token}"
                else:
                    return {
                        "success": False,
                        "error": f"Login succeeded but no token found in response. Keys: {list(login_data.keys()) if isinstance(login_data, dict) else 'N/A'}",
                    }
            else:
                return {
                    "success": False,
                    "error": f"Auth login returned HTTP {login_resp.status_code}: {login_resp.text[:300]}",
                }
        except Exception as e:
            return {
                "success": False,
                "error": f"Auth login failed: {str(e)}",
            }

    _SUCCESS_CODES = {200, 201, 202}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            if method.upper() == "POST":
                post_headers = {**req_headers, "Content-Type": "application/json"}
                resp = await client.post(url, headers=post_headers, json=params or {})
            else:
                resp = await client.get(url, headers=req_headers, params=params or {})

        content_type = resp.headers.get("content-type", "")

        if resp.status_code not in _SUCCESS_CODES:
            error_body = _safe_error_text(resp)
            return {
                "success": False,
                "status_code": resp.status_code,
                "content_type": content_type,
                "error": f"HTTP {resp.status_code}: {error_body}",
            }

        preview = None
        row_count = None

        if "json" in content_type:
            try:
                data = resp.json()
                records = _normalize_records(data)
                row_count = len(records)
                preview = records[:5]
                if not preview and isinstance(data, dict):
                    preview = [data]
                    row_count = 1
            except Exception:
                preview = [{"raw": resp.text[:1000]}]
        else:
            lines = resp.text.strip().split("\n")
            row_count = max(0, len(lines) - 1)
            preview = [{"line": line} for line in lines[:6]]

        return {
            "success": True,
            "status_code": resp.status_code,
            "content_type": content_type,
            "row_count": row_count,
            "preview": preview,
        }

    except httpx.TimeoutException:
        return {"success": False, "error": "Request timed out (30s)"}
    except Exception as e:
        return {"success": False, "error": f"Request failed: {str(e)}"}


# ============ Helpers ============

def _is_empty_response(data: Any) -> bool:
    """Check if the API returned effectively empty data."""
    if data is None:
        return True
    if isinstance(data, list) and len(data) == 0:
        return True
    if isinstance(data, str) and data.strip() == "":
        return True
    if isinstance(data, dict):
        records = _normalize_records(data)
        if not records:
            return False  # scalar dict is still valid data
    return False


def _safe_error_text(resp: httpx.Response, max_len: int = 500) -> str:
    """Extract error text from a response, handling binary content safely."""
    try:
        return resp.text[:max_len]
    except Exception:
        return f"<{len(resp.content)} bytes, content-type={resp.headers.get('content-type', 'unknown')}>"


def _normalize_records(data: Any) -> List[Any]:
    """
    Extract a flat list of record dicts from various API response shapes.
    Handles: list of dicts, wrapped list (World Bank), dict with data/records/results key.
    """
    if isinstance(data, list):
        from app.agent.converter import _extract_records_from_list
        return _extract_records_from_list(data)
    if isinstance(data, dict):
        for key in ("data", "records", "results", "items", "rows",
                     "entries", "values", "response", "content"):
            val = data.get(key)
            if isinstance(val, list):
                from app.agent.converter import _extract_records_from_list
                return _extract_records_from_list(val)
        return [data]
    return []


def _estimate_row_count(data: Any) -> Optional[int]:
    """Estimate the number of data rows in a response."""
    if isinstance(data, list):
        return len(data)
    elif isinstance(data, dict):
        for key in ("data", "records", "results", "items", "rows",
                     "entries", "values", "response", "content"):
            if key in data and isinstance(data[key], list):
                return len(data[key])
    return None
