"""
Converter — API response → CSV file conversion.

Converts API fetch results into CSV files that can be fed into the existing
ingestion pipeline as structured data. All API data is converted to CSV
to ensure it routes through the SQL pipeline (vector pipeline only accepts PDFs).

Pattern: Module-level async functions + @dataclass (adapter-style)
"""
import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.agent.source_config import APIEndpointConfig

logger = logging.getLogger(__name__)


# ============ Data Classes ============

@dataclass
class ConvertedFile:
    """Result from converting a fetch result to a file."""
    success: bool
    file_path: Optional[str] = None
    filename: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ============ Public Functions ============

async def convert_to_file(
    data: Any,
    source_id: str,
    api_config: APIEndpointConfig,
    output_dir: str,
    content_type: Optional[str] = None,
    period_label: Optional[str] = None,
) -> ConvertedFile:
    """
    Convert fetched API data into a CSV file.

    All API data is converted to CSV to ensure structured classification
    and routing through the SQL pipeline.

    Conversion rules:
    - JSON array of objects → multi-row CSV
    - Wrapped JSON ({"data": [...]}) → extract array → multi-row CSV
    - Dict with key-value sub-dict → multi-row CSV (key, value columns)
    - Flat dict → single-row CSV (keys become columns)
    - CSV response → CSV passthrough

    Args:
        data: The fetched data (parsed JSON or raw text).
        source_id: Source identifier.
        api_config: API endpoint config.
        output_dir: Directory to write the output file.
        content_type: HTTP content-type from response.
        period_label: Optional period label for the filename (e.g. "2025" or "2025-01_2025-03").

    Returns:
        ConvertedFile with the path to the written file.
    """
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Always produce CSV
        filename = _generate_filename(source_id, api_config.id, period_label=period_label)
        file_path = output_path / filename

        metadata = dict(api_config.metadata)
        metadata["source_id"] = source_id
        metadata["api_id"] = api_config.id
        metadata["api_name"] = api_config.name
        if api_config.description:
            metadata["description"] = api_config.description

        if data is None:
            return ConvertedFile(
                success=False,
                error="No data to convert (data is None)",
            )

        if api_config.output_format == "csv" and isinstance(data, str):
            if not data.strip():
                return ConvertedFile(
                    success=False,
                    error="CSV response is empty",
                )
            file_path.write_text(data, encoding="utf-8")
            logger.info(f"Wrote CSV passthrough: {filename}")

        elif isinstance(data, list):
            if not data:
                return ConvertedFile(
                    success=False,
                    error="API returned an empty list — nothing to convert",
                )
            records = _extract_records_from_list(data)
            if not records:
                return ConvertedFile(
                    success=False,
                    error="Could not extract any records from list response",
                )
            _write_json_array_to_csv(records, file_path)
            logger.info(f"Converted JSON array → CSV: {filename} ({len(records)} rows)")

        elif isinstance(data, dict):
            tabular_data = _extract_tabular_data(data)
            if tabular_data:
                _write_json_array_to_csv(tabular_data, file_path)
                logger.info(f"Extracted nested array → CSV: {filename} ({len(tabular_data)} rows)")
            else:
                rows = _flatten_dict_to_rows(data)
                _write_json_array_to_csv(rows, file_path)
                logger.info(f"Flattened JSON dict → CSV: {filename} ({len(rows)} rows)")

        elif isinstance(data, str):
            if not data.strip():
                return ConvertedFile(
                    success=False,
                    error="API returned empty text response",
                )
            file_path.write_text(data, encoding="utf-8")
            logger.info(f"Wrote raw text file: {filename}")

        else:
            return ConvertedFile(
                success=False,
                error=f"Unsupported data type: {type(data).__name__}",
            )

        return ConvertedFile(
            success=True,
            file_path=str(file_path.resolve()),
            filename=filename,
            metadata=metadata,
        )

    except Exception as e:
        logger.error(f"Conversion failed for {source_id}/{api_config.id}: {e}")
        return ConvertedFile(
            success=False,
            error=f"Conversion failed: {str(e)}",
        )


# ============ Helpers ============

def _generate_filename(
    source_id: str, api_id: str, format: str = "csv",
    period_label: Optional[str] = None,
) -> str:
    """
    Generate a CSV filename for the converted file.

    Without period: {source_id}_{api_id}_{YYYYMMDD}.csv
    With period:    {source_id}_{api_id}_{period_label}_{YYYYMMDD}.csv
    """
    date_str = datetime.now().strftime("%Y%m%d")
    if period_label:
        safe_label = period_label.replace("/", "-").replace(" ", "_")
        return f"{source_id}_{api_id}_{safe_label}_{date_str}.csv"
    return f"{source_id}_{api_id}_{date_str}.csv"


def _write_json_array_to_csv(data: List[Dict[str, Any]], file_path: Path):
    """
    Write a JSON array of objects to a CSV file.
    Uses csv module (no pandas dependency).

    Args:
        data: List of dicts representing rows.
        file_path: Path to write the CSV file.
    """
    if not data:
        file_path.write_text("no_data\n", encoding="utf-8")
        return

    all_keys = []
    seen_keys = set()
    for row in data:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            if key not in seen_keys:
                all_keys.append(key)
                seen_keys.add(key)

    if not all_keys:
        file_path.write_text("no_data\n", encoding="utf-8")
        return

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            if not isinstance(row, dict):
                continue
            flat_row = {}
            for k, v in row.items():
                if v is None:
                    flat_row[k] = ""
                elif isinstance(v, (dict, list)):
                    flat_row[k] = json.dumps(v, default=str)
                else:
                    flat_row[k] = v
            writer.writerow(flat_row)


def _extract_tabular_data(data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    Try to extract a list of records from common JSON response wrappers.

    Checks keys: data, records, results, items, rows, entries, values, response, content

    Args:
        data: JSON response dict.

    Returns:
        List of record dicts if found, None otherwise.
    """
    for key in ("data", "records", "results", "items", "rows",
                 "entries", "values", "response", "content"):
        val = data.get(key)
        if isinstance(val, list) and val:
            dicts = [r for r in val if isinstance(r, dict)]
            if dicts:
                return dicts
    return None


def _extract_records_from_list(data: list) -> List[Dict[str, Any]]:
    """
    Extract a list of record dicts from a parsed list.

    Handles normal lists of dicts: [{}, {}, ...]
    And wrapped lists of lists: [{}, [{}, {}, ...]] (World Bank style)

    Args:
        data: A root JSON array.

    Returns:
        List of dicts for CSV conversion.
    """
    if not data:
        return []

    # Case 1: Standard flat list of dicts
    if all(isinstance(x, dict) for x in data):
        return data

    # Case 2: List containing a nested list of dicts (World Bank API style)
    if (len(data) >= 2 and isinstance(data[0], dict)
            and isinstance(data[1], list)):
        items = data[1]
        if items:
            dicts = [r for r in items if isinstance(r, dict)]
            if dicts:
                return dicts

    # Fallback: flatten the array elements into dicts if they aren't already
    # For example, a list of strings: ["a", "b"] -> [{"value": "a"}, {"value": "b"}]
    flat_data = []
    for item in data:
        if isinstance(item, dict):
            flat_row = {}
            for k, v in item.items():
                if isinstance(v, (dict, list)):
                    flat_row[k] = json.dumps(v, default=str)
                else:
                    flat_row[k] = v
            flat_data.append(flat_row)
        else:
            flat_data.append({"value": item})
            
    return flat_data


def _flatten_dict_to_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten a non-tabular JSON dict into CSV rows.

    Strategy:
    1. Look for a large nested dict of key-value pairs (e.g. exchange rates)
       → expand into rows: {key_name: key, value_name: value, ...context}
    2. Otherwise → single-row CSV with all top-level keys as columns,
       nested values stringified.

    Examples:
        {"rates": {"USD": 0.01, "EUR": 0.009}, "base": "INR"}
        → [{"currency": "USD", "rate": 0.01, "base": "INR"},
           {"currency": "EUR", "rate": 0.009, "base": "INR"}, ...]

        {"temperature": 21.8, "windspeed": 3.7, "is_day": 0}
        → [{"temperature": 21.8, "windspeed": 3.7, "is_day": 0}]
    """
    # Strategy 1: Find a large nested dict that looks like key-value data
    best_key = None
    best_size = 0

    for key, value in data.items():
        if isinstance(value, dict) and len(value) > 3:
            # This looks like a dict of key-value pairs (e.g., currency rates)
            # Check that most values are scalars (not nested dicts/lists)
            scalar_count = sum(
                1 for v in value.values()
                if isinstance(v, (int, float, str, bool, type(None)))
            )
            if scalar_count > len(value) * 0.7 and len(value) > best_size:
                best_key = key
                best_size = len(value)

    if best_key and best_size > 3:
        # Expand the nested dict into rows
        nested = data[best_key]

        # Collect context fields (all top-level non-dict, non-list values)
        context = {}
        for k, v in data.items():
            if k == best_key:
                continue
            if isinstance(v, (str, int, float, bool)):
                context[k] = v
            elif isinstance(v, dict) and len(v) <= 3:
                # Small nested dicts → flatten with prefix
                for sk, sv in v.items():
                    if isinstance(sv, (str, int, float, bool)):
                        context[f"{k}_{sk}"] = sv

        rows = []
        for item_key, item_value in nested.items():
            row = {best_key + "_key": item_key, best_key + "_value": item_value}
            row.update(context)
            rows.append(row)

        logger.info(f"Flattened nested dict '{best_key}' → {len(rows)} rows")
        return rows

    # Strategy 2: Single-row CSV — flatten everything to one row
    flat_row = {}
    for k, v in data.items():
        if isinstance(v, (str, int, float, bool, type(None))):
            flat_row[k] = v
        elif isinstance(v, dict):
            # Flatten nested dict with prefix
            for sk, sv in v.items():
                if isinstance(sv, (str, int, float, bool, type(None))):
                    flat_row[f"{k}_{sk}"] = sv
                else:
                    flat_row[f"{k}_{sk}"] = json.dumps(sv, default=str)
        elif isinstance(v, list):
            flat_row[k] = json.dumps(v, default=str)
        else:
            flat_row[k] = str(v)

    return [flat_row]
