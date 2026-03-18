"""
Live API test — verify MoSPI actually respects period (year) filtering.

Fetches CPI data for a single year and compares row count against an
unfiltered fetch. No DB writes, no pipeline feed. Read-only HTTP only.

Run from the ingestion-pipeline directory:
    python test_period_live.py
"""
import asyncio
import httpx


MOSPI_BASE = "https://api.mospi.gov.in"
CPI_ENDPOINT = "/api/cpi/getCPIIndex"
WPI_ENDPOINT = "/api/wpi/getWpiRecords"
TIMEOUT = httpx.Timeout(120.0)


def _extract_rows(data):
    """Pull the record list out of MoSPI's wrapper format."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "records", "results", "items", "rows"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


async def fetch_rows(endpoint: str, params: dict) -> dict:
    """Fetch from MoSPI and return summary (no side effects)."""
    url = MOSPI_BASE + endpoint
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, params=params)

    if resp.status_code != 200:
        return {
            "params": params,
            "status": resp.status_code,
            "error": resp.text[:300],
        }

    data = resp.json()
    rows = _extract_rows(data)
    row_count = len(rows)
    sample = rows[:2] if rows else []

    return {
        "params": params,
        "status": 200,
        "row_count": row_count,
        "sample_keys": list(sample[0].keys()) if sample and isinstance(sample[0], dict) else [],
        "sample": sample,
    }


async def main():
    print("=" * 70)
    print("LIVE TEST: MoSPI CPI & WPI — Period Filtering Verification")
    print("  Param: lowercase 'year' | Response wrapper: {\"data\": [...]}")
    print("=" * 70)

    # -----------------------------------------------------------------
    # CPI Tests
    # -----------------------------------------------------------------
    print("\n--- CPI Index ---")

    print("\n[1] Unfiltered fetch (limit=50000)...")
    full = await fetch_rows(CPI_ENDPOINT, {
        "Series": "Current_series_2012",
        "limit": 50000,
    })
    if full["status"] != 200:
        print(f"    FAILED: HTTP {full['status']} — {full.get('error')}")
        return
    print(f"    Rows: {full['row_count']}")
    print(f"    Columns: {full['sample_keys']}")

    test_year = "2024"
    print(f"\n[2] Filtered fetch (year={test_year}, limit=50000)...")
    single = await fetch_rows(CPI_ENDPOINT, {
        "Series": "Current_series_2012",
        "year": test_year,
        "limit": 50000,
    })
    if single["status"] != 200:
        print(f"    FAILED: HTTP {single['status']} — {single.get('error')}")
        return
    print(f"    Rows: {single['row_count']}")

    test_year_2 = "2020"
    print(f"\n[3] Filtered fetch (year={test_year_2}, limit=50000)...")
    single_2 = await fetch_rows(CPI_ENDPOINT, {
        "Series": "Current_series_2012",
        "year": test_year_2,
        "limit": 50000,
    })
    if single_2["status"] != 200:
        print(f"    FAILED: HTTP {single_2['status']} — {single_2.get('error')}")
        return
    print(f"    Rows: {single_2['row_count']}")

    test_year_future = "2099"
    print(f"\n[4] Future year (year={test_year_future}, limit=50000)...")
    future = await fetch_rows(CPI_ENDPOINT, {
        "Series": "Current_series_2012",
        "year": test_year_future,
        "limit": 50000,
    })
    if future["status"] != 200:
        print(f"    FAILED: HTTP {future['status']} — {future.get('error')}")
        return
    print(f"    Rows: {future['row_count']}")

    # -----------------------------------------------------------------
    # WPI Tests
    # -----------------------------------------------------------------
    print("\n--- WPI Records ---")

    print("\n[5] WPI unfiltered (limit=50000)...")
    wpi_full = await fetch_rows(WPI_ENDPOINT, {
        "Format": "JSON",
        "limit": 50000,
    })
    if wpi_full["status"] != 200:
        print(f"    FAILED: HTTP {wpi_full['status']}")
        return
    print(f"    Rows: {wpi_full['row_count']}")

    print(f"\n[6] WPI filtered (year=2024, limit=50000)...")
    wpi_filt = await fetch_rows(WPI_ENDPOINT, {
        "Format": "JSON",
        "year": "2024",
        "limit": 50000,
    })
    if wpi_filt["status"] != 200:
        print(f"    FAILED: HTTP {wpi_filt['status']}")
        return
    print(f"    Rows: {wpi_filt['row_count']}")

    # -----------------------------------------------------------------
    # Verdict
    # -----------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)

    print(f"\n  CPI (unfiltered, limit=50k): {full['row_count']} rows")
    print(f"  CPI year={test_year}:             {single['row_count']} rows")
    print(f"  CPI year={test_year_2}:             {single_2['row_count']} rows")
    print(f"  CPI year={test_year_future}:             {future['row_count']} rows")

    cpi_ok = single["row_count"] < full["row_count"] and single["row_count"] > 0
    if cpi_ok:
        ratio = full["row_count"] / max(single["row_count"], 1)
        print(f"\n  ✅ CPI: API RESPECTS 'year' param — "
              f"{ratio:.1f}x reduction for year={test_year}")
    else:
        print(f"\n  ❌ CPI: 'year' param did NOT filter")

    if future["row_count"] == 0:
        print(f"  ✅ CPI: Future year ({test_year_future}) correctly returns 0 rows")
    else:
        print(f"  ⚠️  CPI: Future year ({test_year_future}) returned "
              f"{future['row_count']} rows")

    print(f"\n  WPI (unfiltered, limit=50k): {wpi_full['row_count']} rows")
    print(f"  WPI year=2024:              {wpi_filt['row_count']} rows")

    wpi_ok = wpi_filt["row_count"] < wpi_full["row_count"] and wpi_filt["row_count"] > 0
    if wpi_ok:
        ratio = wpi_full["row_count"] / max(wpi_filt["row_count"], 1)
        print(f"\n  ✅ WPI: API RESPECTS 'year' param — "
              f"{ratio:.1f}x reduction for year=2024")
    else:
        print(f"\n  ⚠️  WPI: 'year' param may not filter "
              f"(full={wpi_full['row_count']}, filt={wpi_filt['row_count']})")

    # Incremental savings estimate
    if cpi_ok:
        one_year = single["row_count"]
        lookback_1 = one_year * 2  # current + previous year
        print(f"\n  📊 INCREMENTAL SAVINGS ESTIMATE (CPI):")
        print(f"     Full fetch:        {full['row_count']:>8} rows")
        print(f"     Incremental (L=1): {lookback_1:>8} rows  "
              f"({100 - lookback_1/full['row_count']*100:.0f}% reduction)")

    # Show a sample row
    if single.get("sample"):
        print(f"\n  Sample CPI row (year={test_year}):")
        for k, v in list(single["sample"][0].items())[:8]:
            print(f"    {k}: {v}")


if __name__ == "__main__":
    asyncio.run(main())
