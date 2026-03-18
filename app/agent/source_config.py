"""
Source Config — YAML config loader + Pydantic models + unified source resolution.

Loads pre-configured data sources from YAML files under app/agent/sources/
and runtime-registered sources from the source_registry DB table.
Merges both into a unified view for the orchestrator.

Pattern: Pydantic models + module-level loader functions (like schemas.py)
"""
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def _safe_json_dict(val: Any) -> dict:
    """Parse a value that might be a JSON string or already a dict."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


# ============ Pydantic Models ============

class AuthConfig(BaseModel):
    """Authentication configuration for a data source."""
    type: str = "none"                          # token | api_key | none
    login_url: Optional[str] = None
    signup_url: Optional[str] = None
    token_expiry_minutes: int = 14
    credentials_env: Dict[str, str] = Field(default_factory=dict)


class PeriodConfig(BaseModel):
    """
    Optional period-aware fetching configuration for an API endpoint.

    Enables incremental fetching by injecting time-based parameters so that
    subsequent runs only request data newer than the last successful fetch.

    Two modes:
      - Single param: one parameter carries the period value (e.g. Year=2025).
      - Range params: two parameters define a from/to window
        (e.g. from_date=2025-01-01&to_date=2025-03-31).
    """
    type: str                                    # daily | monthly | quarterly | yearly
    param_name: Optional[str] = None             # single-param mode (e.g. "Year")
    param_format: str = "%Y-%m-%d"               # strftime format for the value
    range_params: Optional[Dict[str, str]] = None  # {"from": "from_date", "to": "to_date"}
    range_format: Optional[str] = None           # strftime format for range values (defaults to param_format)
    lookback: int = 1                            # re-fetch last N periods for overlap safety
    initial_start: Optional[str] = None          # earliest period to fetch on first run (e.g. "2013-01")


class APIEndpointConfig(BaseModel):
    """Configuration for a single API endpoint within a source."""
    id: str
    name: str
    endpoint: str
    method: str = "GET"
    description: Optional[str] = None
    output_format: str = "json"                  # json | csv
    params: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, str] = Field(default_factory=dict)
    schedule: str = "manual"                     # daily | weekly | monthly | manual
    enabled: bool = True
    pagination: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    period: Optional[PeriodConfig] = None


class SourceConfig(BaseModel):
    """Full data source configuration."""
    source_id: str
    name: str
    base_url: str
    auth: AuthConfig = Field(default_factory=AuthConfig)
    apis: List[APIEndpointConfig] = Field(default_factory=list)
    enabled: bool = True
    origin: str = "yaml"                         # yaml | db


# ============ YAML Config Loading ============

_SOURCES_DIR = Path(__file__).parent / "sources"


def load_source_configs() -> Dict[str, SourceConfig]:
    """
    Load all YAML source config files from app/agent/sources/.

    Returns:
        Dict mapping source_id -> SourceConfig for all YAML sources.
    """
    configs: Dict[str, SourceConfig] = {}

    if not _SOURCES_DIR.exists():
        logger.warning(f"Sources directory not found: {_SOURCES_DIR}")
        return configs

    for yaml_path in sorted(_SOURCES_DIR.glob("*.yaml")):
        try:
            with open(yaml_path, "r") as f:
                raw = yaml.safe_load(f)

            if not raw or not isinstance(raw, dict):
                logger.warning(f"Skipping empty/invalid YAML: {yaml_path.name}")
                continue

            config = _parse_yaml_source(raw)
            config.origin = "yaml"
            configs[config.source_id] = config
            logger.info(f"Loaded YAML source config: {config.source_id} ({len(config.apis)} APIs)")

        except Exception as e:
            logger.error(f"Failed to load source config {yaml_path.name}: {e}")

    return configs


def get_source_config(source_id: str) -> Optional[SourceConfig]:
    """
    Get a single YAML source config by ID.

    Args:
        source_id: The source identifier.

    Returns:
        SourceConfig if found in YAML sources, None otherwise.
    """
    configs = load_source_configs()
    return configs.get(source_id)


def _parse_yaml_source(raw: Dict[str, Any]) -> SourceConfig:
    """Parse raw YAML dict into a SourceConfig."""
    auth_raw = raw.get("auth", {})
    auth = AuthConfig(
        type=auth_raw.get("type", "none"),
        login_url=auth_raw.get("login_url"),
        signup_url=auth_raw.get("signup_url"),
        token_expiry_minutes=auth_raw.get("token_expiry_minutes", 14),
        credentials_env=auth_raw.get("credentials_env", {}),
    )

    apis = []
    for api_raw in raw.get("apis", []):
        period_raw = api_raw.get("period")
        period_cfg = None
        if period_raw and isinstance(period_raw, dict):
            period_cfg = PeriodConfig(
                type=period_raw["type"],
                param_name=period_raw.get("param_name"),
                param_format=period_raw.get("param_format", "%Y-%m-%d"),
                range_params=period_raw.get("range_params"),
                range_format=period_raw.get("range_format"),
                lookback=period_raw.get("lookback", 1),
                initial_start=period_raw.get("initial_start"),
            )

        api = APIEndpointConfig(
            id=api_raw["id"],
            name=api_raw["name"],
            endpoint=api_raw["endpoint"],
            method=api_raw.get("method", "GET"),
            description=api_raw.get("description"),
            output_format=api_raw.get("output_format", "json"),
            params=api_raw.get("params") or {},
            headers=api_raw.get("headers") or {},
            schedule=api_raw.get("schedule", "manual"),
            enabled=api_raw.get("enabled", True),
            pagination=api_raw.get("pagination") or {},
            metadata=api_raw.get("metadata") or {},
            period=period_cfg,
        )
        apis.append(api)

    return SourceConfig(
        source_id=raw["source_id"],
        name=raw["name"],
        base_url=raw["base_url"],
        auth=auth,
        apis=apis,
        enabled=raw.get("enabled", True),
    )


# ============ Unified Source Resolution ============

async def get_all_sources() -> Dict[str, SourceConfig]:
    """
    Merge YAML configs and DB-registered sources.
    YAML wins on conflict (same source_id).

    Returns:
        Dict mapping source_id -> SourceConfig for all sources.
    """
    yaml_sources = load_source_configs()
    db_sources = await _load_db_sources()

    # DB first, then YAML overwrites on conflict
    merged: Dict[str, SourceConfig] = {}
    for sid, cfg in db_sources.items():
        merged[sid] = cfg
    for sid, cfg in yaml_sources.items():
        merged[sid] = cfg

    return merged


async def get_source(source_id: str) -> Optional[SourceConfig]:
    """
    Resolve a single source — checks YAML first, then DB.

    Args:
        source_id: The source identifier.

    Returns:
        SourceConfig if found, None otherwise.
    """
    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        return yaml_cfg
    return await _get_db_source(source_id)


async def _load_db_sources() -> Dict[str, SourceConfig]:
    """Load all enabled DB-registered sources, convert to SourceConfig."""
    try:
        from app.models.db import get_all_db_sources
        rows = await get_all_db_sources()
    except Exception as e:
        logger.warning(f"Could not load DB sources: {e}")
        return {}

    result: Dict[str, SourceConfig] = {}
    for row in rows:
        try:
            cfg = _db_row_to_source_config(row)
            if cfg:
                result[cfg.source_id] = cfg
        except Exception as e:
            logger.error(f"Failed to parse DB source row: {e}")

    return result


async def _get_db_source(source_id: str) -> Optional[SourceConfig]:
    """Load a single DB source by source_id."""
    try:
        from app.models.db import get_db_source
        row = await get_db_source(source_id)
    except Exception as e:
        logger.warning(f"Could not load DB source {source_id}: {e}")
        return None

    if not row:
        return None

    try:
        return _db_row_to_source_config(row)
    except Exception as e:
        logger.error(f"Failed to parse DB source {source_id}: {e}")
        return None


def _db_row_to_source_config(row: Dict[str, Any]) -> Optional[SourceConfig]:
    """
    Convert a DB source_registry row (with nested endpoints) to a SourceConfig.

    Args:
        row: Dict with source_registry fields + 'endpoints' list.

    Returns:
        SourceConfig or None if parsing fails.
    """
    if not row:
        return None

    # Build auth config from flat DB fields
    auth_creds = _safe_json_dict(row.get("auth_credentials"))
    auth = AuthConfig(
        type=row.get("auth_type", "none"),
        login_url=row.get("auth_login_url"),
        token_expiry_minutes=row.get("auth_token_expiry_minutes", 14),
        credentials_env={str(k): str(v) for k, v in auth_creds.items()} if auth_creds else {},
    )

    # Build API endpoints
    apis = []
    for ep in row.get("endpoints", []):
        period_raw = _safe_json_dict(ep.get("period_config"))
        period_cfg = None
        if period_raw and period_raw.get("type"):
            period_cfg = PeriodConfig(
                type=period_raw["type"],
                param_name=period_raw.get("param_name"),
                param_format=period_raw.get("param_format", "%Y-%m-%d"),
                range_params=period_raw.get("range_params"),
                range_format=period_raw.get("range_format"),
                lookback=period_raw.get("lookback", 1),
                initial_start=period_raw.get("initial_start"),
            )

        api = APIEndpointConfig(
            id=ep["api_id"],
            name=ep["name"],
            endpoint=ep["endpoint"],
            method=ep.get("method", "GET"),
            description=ep.get("description"),
            output_format=ep.get("output_format", "json"),
            params=_safe_json_dict(ep.get("params")),
            headers=_safe_json_dict(ep.get("headers")),
            schedule=ep.get("schedule", "manual"),
            enabled=ep.get("enabled", True),
            metadata=_safe_json_dict(ep.get("metadata")),
            period=period_cfg,
        )
        apis.append(api)

    return SourceConfig(
        source_id=row["source_id"],
        name=row["name"],
        base_url=row["base_url"],
        auth=auth,
        apis=apis,
        enabled=row.get("enabled", True),
        origin="db",
    )
