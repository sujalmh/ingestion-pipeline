"""
Recover stuck SQL files in inbound_files_metadata.

Use this when files are stuck in `processing` after approval/batch-approval.
The script checks SQL pipeline job status and syncs records to terminal states.

Usage:
  python scripts/recover_stuck_sql_processing.py
    python scripts/recover_stuck_sql_processing.py --dry-run
    python scripts/recover_stuck_sql_processing.py --apply
  python scripts/recover_stuck_sql_processing.py --mark-missing-done
  python scripts/recover_stuck_sql_processing.py --mark-missing-failed

Required env vars:
  DATABASE_URL
Optional env vars:
  SQL_PIPELINE_API_URL (default: http://localhost:8000)
"""

import argparse
import asyncio
import os
from typing import Dict, List, Tuple

import asyncpg
import httpx


TERMINAL_SUCCESS = {"completed", "incremental_load_completed"}
TERMINAL_FAILURE = {"failed", "rejected"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover stuck SQL processing files")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not update DB; only show what would be updated and what remains pending.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply DB updates (default behavior is apply unless --dry-run is set).",
    )
    parser.add_argument(
        "--mark-missing-done",
        action="store_true",
        help="Mark rows as done when SQL job is missing (404 from SQL API).",
    )
    parser.add_argument(
        "--mark-missing-failed",
        action="store_true",
        help="Mark rows as failed when SQL job is missing (404 from SQL API).",
    )
    return parser.parse_args()


async def fetch_processing_rows(conn: asyncpg.Connection) -> List[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT CAST(id AS TEXT) AS id,
               original_filename,
               status,
                             error_message,
                             source_type,
                             source_identifier,
                             source_url,
                             operational_metadata_id
        FROM inbound_files_metadata
        WHERE status = 'processing'
          AND routed_to LIKE 'sql_%'
          AND error_message LIKE 'sql_job_id:%'
        ORDER BY created_at DESC
        """
    )


async def mark_done(conn: asyncpg.Connection, file_id: str) -> None:
    await conn.execute(
        """
        UPDATE inbound_files_metadata
        SET status = 'done',
            error_message = NULL,
            processing_completed_at = now()
        WHERE id = $1::uuid
        """,
        file_id,
    )


async def mark_failed(conn: asyncpg.Connection, file_id: str, reason: str) -> None:
    await conn.execute(
        """
        UPDATE inbound_files_metadata
        SET status = 'failed',
            error_message = $2,
            processing_completed_at = now()
        WHERE id = $1::uuid
        """,
        file_id,
        reason,
    )


async def get_sql_status(client: httpx.AsyncClient, base_url: str, job_id: str) -> Tuple[int, Dict]:
    resp = await client.get(f"{base_url.rstrip('/')}/status/{job_id}")
    payload = {}
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    return resp.status_code, payload


def _extract_approval_metadata(payload: Dict) -> Dict[str, str]:
    """Best-effort extraction of approval metadata from SQL status payload."""
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        result = {}

    preview = payload.get("preview") if isinstance(payload, dict) else None
    if not isinstance(preview, dict):
        preview = {}

    business_meta = None
    bm_value = result.get("business_metadata")
    if isinstance(bm_value, dict):
        business_meta = str(bm_value)
    elif bm_value is not None:
        business_meta = str(bm_value)

    return {
        "source": str(result.get("source") or preview.get("source") or ""),
        "source_url": str(result.get("source_url") or preview.get("source_url") or ""),
        "released_on": str(result.get("released_on") or preview.get("released_on") or ""),
        "updated_on": str(result.get("updated_on") or preview.get("updated_on") or ""),
        "business_metadata": business_meta or "",
    }


def _print_remaining(remaining: List[Dict]) -> None:
    print("\nRemaining processing items")
    if not remaining:
        print("  none")
        return

    for item in remaining:
        print(
            f"  - file={item['filename']} id={item['file_id']} job={item['job_id']} "
            f"sql_status={item['sql_status']} source_present={item['has_source']} "
            f"source_url_present={item['has_source_url']} released_on_present={item['has_released_on']} "
            f"updated_on_present={item['has_updated_on']} business_metadata_present={item['has_business_metadata']}"
        )

        if item.get("source_identifier") or item.get("inbound_source_url"):
            print(
                f"      inbound source_identifier={item.get('source_identifier') or ''} "
                f"inbound source_url={item.get('inbound_source_url') or ''}"
            )


async def main() -> None:
    args = parse_args()

    dry_run = args.dry_run and not args.apply
    if args.dry_run and args.apply:
        raise SystemExit("Use only one of --dry-run or --apply")

    if args.mark_missing_done and args.mark_missing_failed:
        raise SystemExit("Use only one of --mark-missing-done or --mark-missing-failed")

    database_url = os.getenv("DATABASE_URL")
    sql_api_url = os.getenv("SQL_PIPELINE_API_URL", "http://localhost:8000")

    if not database_url:
        raise SystemExit("DATABASE_URL is required")

    conn = await asyncpg.connect(database_url)
    try:
        rows = await fetch_processing_rows(conn)
        if not rows:
            print("No SQL processing rows found.")
            return

        print(f"Found {len(rows)} SQL rows in processing.")

        done_count = 0
        failed_count = 0
        pending_count = 0
        missing_count = 0
        remaining_items: List[Dict] = []

        async with httpx.AsyncClient(timeout=15.0) as client:
            for row in rows:
                file_id = row["id"]
                filename = row["original_filename"]
                err = row["error_message"] or ""
                job_id = err.replace("sql_job_id:", "", 1)

                try:
                    status_code, payload = await get_sql_status(client, sql_api_url, job_id)
                except Exception as ex:
                    print(f"[WARN] {filename} ({file_id}) job={job_id}: SQL API unreachable: {ex}")
                    pending_count += 1
                    continue

                if status_code == 200:
                    sql_status = payload.get("status")
                    approval_meta = _extract_approval_metadata(payload)
                    has_source = bool(approval_meta["source"].strip())
                    has_source_url = bool(approval_meta["source_url"].strip())
                    has_released_on = bool(approval_meta["released_on"].strip())
                    has_updated_on = bool(approval_meta["updated_on"].strip())
                    has_business_metadata = bool(approval_meta["business_metadata"].strip())

                    if sql_status in TERMINAL_SUCCESS:
                        if not dry_run:
                            await mark_done(conn, file_id)
                        done_count += 1
                        prefix = "[DRY-DONE]" if dry_run else "[DONE]"
                        print(
                            f"{prefix} {filename} ({file_id}) job={job_id} sql_status={sql_status} "
                            f"source={has_source} source_url={has_source_url} released_on={has_released_on} "
                            f"updated_on={has_updated_on} business_metadata={has_business_metadata}"
                        )
                    elif sql_status in TERMINAL_FAILURE:
                        if not dry_run:
                            await mark_failed(conn, file_id, f"SQL job {sql_status}")
                        failed_count += 1
                        prefix = "[DRY-FAIL]" if dry_run else "[FAIL]"
                        print(
                            f"{prefix} {filename} ({file_id}) job={job_id} sql_status={sql_status} "
                            f"source={has_source} source_url={has_source_url} released_on={has_released_on} "
                            f"updated_on={has_updated_on} business_metadata={has_business_metadata}"
                        )
                    else:
                        pending_count += 1
                        print(
                            f"[PEND] {filename} ({file_id}) job={job_id} sql_status={sql_status} "
                            f"source={has_source} source_url={has_source_url} released_on={has_released_on} "
                            f"updated_on={has_updated_on} business_metadata={has_business_metadata}"
                        )
                        remaining_items.append(
                            {
                                "file_id": file_id,
                                "filename": filename,
                                "job_id": job_id,
                                "sql_status": sql_status,
                                "has_source": has_source,
                                "has_source_url": has_source_url,
                                "has_released_on": has_released_on,
                                "has_updated_on": has_updated_on,
                                "has_business_metadata": has_business_metadata,
                                "source_identifier": row.get("source_identifier"),
                                "inbound_source_url": row.get("source_url"),
                            }
                        )
                    continue

                if status_code == 404:
                    missing_count += 1
                    if args.mark_missing_done:
                        if not dry_run:
                            await mark_done(conn, file_id)
                        done_count += 1
                        prefix = "[DRY-DONE*]" if dry_run else "[DONE*]"
                        print(f"{prefix} {filename} ({file_id}) job={job_id} missing_in_sql_api")
                    elif args.mark_missing_failed:
                        if not dry_run:
                            await mark_failed(conn, file_id, "SQL job missing in SQL API (404)")
                        failed_count += 1
                        prefix = "[DRY-FAIL*]" if dry_run else "[FAIL*]"
                        print(f"{prefix} {filename} ({file_id}) job={job_id} missing_in_sql_api")
                    else:
                        pending_count += 1
                        print(f"[MISS] {filename} ({file_id}) job={job_id} missing_in_sql_api")
                        remaining_items.append(
                            {
                                "file_id": file_id,
                                "filename": filename,
                                "job_id": job_id,
                                "sql_status": "missing_in_sql_api",
                                "has_source": False,
                                "has_source_url": False,
                                "has_released_on": False,
                                "has_updated_on": False,
                                "has_business_metadata": False,
                                "source_identifier": row.get("source_identifier"),
                                "inbound_source_url": row.get("source_url"),
                            }
                        )
                    continue

                pending_count += 1
                print(f"[WARN] {filename} ({file_id}) job={job_id} http_status={status_code}")
                remaining_items.append(
                    {
                        "file_id": file_id,
                        "filename": filename,
                        "job_id": job_id,
                        "sql_status": f"http_{status_code}",
                        "has_source": False,
                        "has_source_url": False,
                        "has_released_on": False,
                        "has_updated_on": False,
                        "has_business_metadata": False,
                        "source_identifier": row.get("source_identifier"),
                        "inbound_source_url": row.get("source_url"),
                    }
                )

        print("\nSummary")
        print(f"  mode:    {'dry-run' if dry_run else 'apply'}")
        print(f"  done:    {done_count}")
        print(f"  failed:  {failed_count}")
        print(f"  pending: {pending_count}")
        print(f"  missing: {missing_count}")

        _print_remaining(remaining_items)

        if missing_count and not (args.mark_missing_done or args.mark_missing_failed):
            print("\nTip: rerun with --mark-missing-done if you know ingestion already finished.")
            print("Tip: rerun with --mark-missing-failed if you want those to be retried.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
