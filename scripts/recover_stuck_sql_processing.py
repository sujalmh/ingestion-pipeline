"""
Recover stuck SQL files in inbound_files_metadata.

Use this when files are stuck in `processing` after approval/batch-approval,
OR to retry files that previously ended up in `failed` state (e.g. due to
a busy SQL server).

Usage:
  python scripts/recover_stuck_sql_processing.py --dry-run
  python scripts/recover_stuck_sql_processing.py --apply
    python scripts/recover_stuck_sql_processing.py --apply --all-dates
  python scripts/recover_stuck_sql_processing.py --apply --reingest-missing \
          --source "SEBI" --source-url "https://www.sebi.gov.in" \
          --released-on "2026-03-17" --updated-on "2026-03-17"
  python scripts/recover_stuck_sql_processing.py --apply --retry-failed \
          --source "SEBI" --source-url "https://www.sebi.gov.in" \
          --released-on "2026-03-17" --updated-on "2026-03-17"
  python scripts/recover_stuck_sql_processing.py --dry-run --retry-sql-upload-errors
  python scripts/recover_stuck_sql_processing.py --apply --retry-sql-upload-errors
  python scripts/recover_stuck_sql_processing.py --apply --retry-sql-upload-errors --all-dates
  python scripts/recover_stuck_sql_processing.py --dry-run --approve-individual \
          --source "SEBI" --source-url "https://www.sebi.gov.in" \
          --released-on "2026-03-18" --updated-on "2026-03-18"
  python scripts/recover_stuck_sql_processing.py --apply --approve-individual \
          --source "SEBI" --source-url "https://www.sebi.gov.in" \
          --released-on "2026-03-18" --updated-on "2026-03-18" --all-dates

Required env vars:
  DATABASE_URL
Optional env vars:
  SQL_PIPELINE_API_URL (default: http://localhost:8000)
"""

import argparse
import asyncio
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import asyncpg
import httpx

# Ensure project root is importable when script is run from any directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    # Preferred: reuse application settings (already loads .env)
    from app.config.settings import settings as app_settings
    from app.models.db import connect_db, disconnect_db
except Exception:
    app_settings = None
    connect_db = None
    disconnect_db = None
    # Fallback: load .env directly so this script can run standalone.
    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
    except Exception:
        pass


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
    parser.add_argument(
        "--reingest-missing",
        action="store_true",
        help="For missing SQL jobs (404), re-upload file, approve with shared metadata, and sync to completion.",
    )
    parser.add_argument("--source", type=str, default="", help="Shared source for reingest approval.")
    parser.add_argument("--source-url", type=str, default="", help="Shared source URL for reingest approval.")
    parser.add_argument("--released-on", type=str, default="", help="Shared released_on date for reingest approval (YYYY-MM-DD).")
    parser.add_argument("--updated-on", type=str, default="", help="Shared updated_on date for reingest approval (YYYY-MM-DD).")
    parser.add_argument("--business-metadata", type=str, default="", help="Optional shared business metadata JSON/string for reingest approval.")
    parser.add_argument("--poll-attempts", type=int, default=24, help="Polling attempts after reingest approval.")
    parser.add_argument("--poll-interval", type=int, default=5, help="Polling interval seconds after reingest approval.")
    parser.add_argument(
        "--for-date",
        type=str,
        default=None,
        help="Only process records created on this date (YYYY-MM-DD). Default is today.",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Process matching SQL rows across all dates instead of only one date.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help=(
            "Retry files whose status is 'failed' (not 'processing'). "
            "Resets them to processing, re-uploads, approves, and polls to completion. "
            "Requires --source, --source-url, --released-on, --updated-on."
        ),
    )
    parser.add_argument(
        "--retry-sql-upload-errors",
        action="store_true",
        help=(
            "Re-upload files that failed at the SQL API upload step "
            "(error_message contains 'SQL API error:', 'SQL Ingestion Pipeline timeout', etc.). "
            "No approval metadata needed — after upload succeeds the file awaits approval normally."
        ),
    )
    parser.add_argument(
        "--approve-individual",
        action="store_true",
        help=(
            "Individually approve files that are in 'processing' state and whose SQL job "
            "is still at 'awaiting_approval' (e.g. after a batch-approve failure). "
            "Requires --source, --source-url, --released-on, --updated-on."
        ),
    )
    parser.add_argument(
        "--exclude-pattern",
        type=str,
        action="append",
        default=[],
        help="Skip files whose name contains this substring (case-insensitive). Can be repeated.",
    )
    return parser.parse_args()


async def fetch_processing_rows(conn: asyncpg.Connection, for_date: Optional[date]) -> List[asyncpg.Record]:
    query = """
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
    """
    params = []
    if for_date is not None:
        query += "\n          AND created_at::date = $1::date"
        params.append(for_date)
    query += "\n        ORDER BY created_at DESC"
    return await conn.fetch(query, *params)


async def fetch_failed_rows(conn: asyncpg.Connection, for_date: Optional[date]) -> List[asyncpg.Record]:
    """Fetch rows in 'failed' state that still have a sql_job_id error message."""
    query = """
        SELECT CAST(id AS TEXT) AS id,
               original_filename,
               status,
               error_message,
               source_type,
               source_identifier,
               source_url,
               operational_metadata_id
        FROM inbound_files_metadata
        WHERE status = 'failed'
          AND routed_to LIKE 'sql_%'
          AND (error_message LIKE 'sql_job_id:%' OR error_message LIKE 'SQL job %')
    """
    params = []
    if for_date is not None:
        query += "\n          AND created_at::date = $1::date"
        params.append(for_date)
    query += "\n        ORDER BY created_at DESC"
    return await conn.fetch(query, *params)


async def fetch_sql_upload_error_rows(conn: asyncpg.Connection, for_date: Optional[date]) -> List[asyncpg.Record]:
    """Fetch rows in 'failed' state where the SQL API upload itself errored (no job id created)."""
    query = """
        SELECT CAST(id AS TEXT) AS id,
               original_filename,
               status,
               error_message,
               source_type,
               source_identifier,
               source_url,
               operational_metadata_id
        FROM inbound_files_metadata
        WHERE status = 'failed'
          AND routed_to LIKE 'sql_%'
          AND (
               error_message LIKE 'SQL API error: %'
            OR error_message LIKE 'SQL Ingestion Pipeline timeout%'
            OR error_message LIKE 'SQL upload failed: %'
            OR error_message LIKE 'SQL pipeline error: %'
          )
    """
    params = []
    if for_date is not None:
        query += "\n          AND created_at::date = $1::date"
        params.append(for_date)
    query += "\n        ORDER BY created_at DESC"
    return await conn.fetch(query, *params)


async def reset_to_processing(conn: asyncpg.Connection, file_id: str, job_id: str) -> None:
    """Reset a failed row back to processing so reingest logic can proceed."""
    await conn.execute(
        """
        UPDATE inbound_files_metadata
        SET status = 'processing',
            error_message = $2,
            processing_completed_at = NULL
        WHERE id = $1::uuid
        """,
        file_id,
        f"sql_job_id:{job_id}",
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


async def _reingest_and_complete(
    file_id: str,
    filename: str,
    sql_api_url: str,
    source: str,
    source_url: str,
    released_on: str,
    updated_on: str,
    business_metadata: str,
    poll_attempts: int,
    poll_interval: int,
) -> Tuple[str, str]:
    """
    Re-upload missing job file, approve it, then sync completion.

    Returns:
        (outcome, message)
        outcome in {'done', 'failed', 'pending'}
    """
    from app.services.sql_adapter import process_sql_pipeline, check_sql_job_completion

    upload_result = await process_sql_pipeline(file_id)
    if not upload_result.success or not upload_result.sql_job_id:
        return "failed", f"reingest upload failed: {upload_result.error or 'no job id'}"

    new_job_id = upload_result.sql_job_id

    form_data = {
        "source": source,
        "source_url": source_url,
        "released_on": released_on,
        "updated_on": updated_on,
    }
    if business_metadata.strip():
        form_data["business_metadata"] = business_metadata

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        approve_resp = await client.post(
            f"{sql_api_url.rstrip('/')}/approve/{new_job_id}",
            data=form_data,
        )

        if approve_resp.status_code != 200:
            # Sync once in case SQL API already moved ahead.
            await check_sql_job_completion(file_id)
            return "failed", f"approval failed http={approve_resp.status_code} body={approve_resp.text[:200]}"

        final_status = "approved"
        for _ in range(max(1, poll_attempts)):
            await asyncio.sleep(max(1, poll_interval))
            status_resp = await client.get(f"{sql_api_url.rstrip('/')}/status/{new_job_id}")
            if status_resp.status_code == 200:
                final_status = (status_resp.json() or {}).get("status", final_status)
                if final_status in TERMINAL_SUCCESS.union(TERMINAL_FAILURE):
                    break

    await check_sql_job_completion(file_id)

    if final_status in TERMINAL_SUCCESS:
        return "done", f"reingest completed (new_job_id={new_job_id}, status={final_status})"
    if final_status in TERMINAL_FAILURE:
        return "failed", f"reingest terminal failure (new_job_id={new_job_id}, status={final_status})"
    return "pending", f"reingest still pending (new_job_id={new_job_id}, status={final_status})"


async def _run_retry_failed(
    conn: asyncpg.Connection,
    args: argparse.Namespace,
    processing_date: date,
    sql_api_url: str,
    dry_run: bool,
) -> None:
    """Re-ingest all rows currently in 'failed' state for the given date."""
    rows = await fetch_failed_rows(conn, processing_date)
    if not rows:
        print(f"No failed SQL rows found for date {processing_date.isoformat()}.")
        return

    print(f"Found {len(rows)} failed SQL rows for date {processing_date.isoformat()}.")

    done_count = 0
    failed_count = 0
    pending_count = 0
    remaining_items: List[Dict] = []

    for row in rows:
        file_id = row["id"]
        filename = row["original_filename"]
        err = row["error_message"] or ""
        # Extract whatever job id is present (best-effort)
        if err.startswith("sql_job_id:"):
            job_id = err.replace("sql_job_id:", "", 1)
        elif err.startswith("SQL job "):
            # e.g. "SQL job failed" — no old job id available
            job_id = "unknown"
        else:
            job_id = err[:60] or "unknown"

        if dry_run:
            pending_count += 1
            print(f"[DRY-RETRY] {filename} ({file_id}) old_job={job_id}")
            remaining_items.append(
                {
                    "file_id": file_id,
                    "filename": filename,
                    "job_id": job_id,
                    "sql_status": "would_retry_failed",
                    "has_source": bool(args.source.strip()),
                    "has_source_url": bool(args.source_url.strip()),
                    "has_released_on": bool(args.released_on.strip()),
                    "has_updated_on": bool(args.updated_on.strip()),
                    "has_business_metadata": bool(args.business_metadata.strip()),
                    "source_identifier": row.get("source_identifier"),
                    "inbound_source_url": row.get("source_url"),
                }
            )
            continue

        # Reset to processing so check_sql_job_completion can update it later.
        await reset_to_processing(conn, file_id, job_id)

        outcome, msg = await _reingest_and_complete(
            file_id=file_id,
            filename=filename,
            sql_api_url=sql_api_url,
            source=args.source.strip(),
            source_url=args.source_url.strip(),
            released_on=args.released_on.strip(),
            updated_on=args.updated_on.strip(),
            business_metadata=args.business_metadata,
            poll_attempts=args.poll_attempts,
            poll_interval=args.poll_interval,
        )
        if outcome == "done":
            done_count += 1
            print(f"[RETRY-DONE] {filename} ({file_id}) old_job={job_id} {msg}")
        elif outcome == "failed":
            failed_count += 1
            print(f"[RETRY-FAIL] {filename} ({file_id}) old_job={job_id} {msg}")
        else:
            pending_count += 1
            print(f"[RETRY-PEND] {filename} ({file_id}) old_job={job_id} {msg}")
            remaining_items.append(
                {
                    "file_id": file_id,
                    "filename": filename,
                    "job_id": job_id,
                    "sql_status": "retry_pending",
                    "has_source": bool(args.source.strip()),
                    "has_source_url": bool(args.source_url.strip()),
                    "has_released_on": bool(args.released_on.strip()),
                    "has_updated_on": bool(args.updated_on.strip()),
                    "has_business_metadata": bool(args.business_metadata.strip()),
                    "source_identifier": row.get("source_identifier"),
                    "inbound_source_url": row.get("source_url"),
                }
            )

    print("\nSummary (retry-failed)")
    print(f"  mode:    {'dry-run' if dry_run else 'apply'}")
    print(f"  done:    {done_count}")
    print(f"  failed:  {failed_count}")
    print(f"  pending: {pending_count}")

    _print_remaining(remaining_items)


async def _run_retry_sql_upload_errors(
    conn: asyncpg.Connection,
    args: argparse.Namespace,
    processing_date: Optional[date],
    date_label: str,
    dry_run: bool,
) -> None:
    """Re-upload SQL files that failed at the upload step (no job_id was ever created)."""
    from app.services.sql_adapter import process_sql_pipeline

    rows = await fetch_sql_upload_error_rows(conn, processing_date)
    if args.exclude_pattern:
        original_count = len(rows)
        rows = [
            r for r in rows
            if not any(pat.lower() in (r["original_filename"] or "").lower() for pat in args.exclude_pattern)
        ]
        skipped = original_count - len(rows)
        if skipped:
            print(f"Excluded {skipped} file(s) matching --exclude-pattern {args.exclude_pattern}.")

    if not rows:
        print(f"No SQL upload-error rows found for {date_label}.")
        return

    print(f"Found {len(rows)} SQL upload-error rows for {date_label}.")

    success_count = 0
    failed_count = 0
    remaining_items: List[Dict] = []

    for row in rows:
        file_id = row["id"]
        filename = row["original_filename"]
        old_error = row["error_message"] or ""

        if dry_run:
            print(f"[DRY-UPLOAD-RETRY] {filename} ({file_id}) old_error={old_error!r}")
            remaining_items.append(
                {
                    "file_id": file_id,
                    "filename": filename,
                    "job_id": "n/a",
                    "sql_status": "would_retry_upload",
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

        result = await process_sql_pipeline(file_id)
        if result.success:
            success_count += 1
            print(
                f"[UPLOAD-OK] {filename} ({file_id}) new_job_id={result.sql_job_id} "
                f"sql_status={result.sql_status}"
            )
        else:
            failed_count += 1
            print(f"[UPLOAD-FAIL] {filename} ({file_id}) error={result.error!r}")
            remaining_items.append(
                {
                    "file_id": file_id,
                    "filename": filename,
                    "job_id": "n/a",
                    "sql_status": f"upload_failed: {result.error}",
                    "has_source": False,
                    "has_source_url": False,
                    "has_released_on": False,
                    "has_updated_on": False,
                    "has_business_metadata": False,
                    "source_identifier": row.get("source_identifier"),
                    "inbound_source_url": row.get("source_url"),
                }
            )

    print("\nSummary (retry-sql-upload-errors)")
    print(f"  mode:    {'dry-run' if dry_run else 'apply'}")
    print(f"  uploaded:  {success_count}  (now in processing, awaiting approval)")
    print(f"  still_failed: {failed_count}")
    if dry_run:
        print(f"  would_retry: {len(rows)}")

    _print_remaining(remaining_items)


async def _run_approve_individual(
    conn: asyncpg.Connection,
    args: argparse.Namespace,
    processing_date: Optional[date],
    date_label: str,
    sql_api_url: str,
    dry_run: bool,
) -> None:
    """
    Individually approve files whose SQL job is still at 'awaiting_approval'.
    Use this to recover after a batch-approve failure.
    """
    rows = await fetch_processing_rows(conn, processing_date)
    if args.exclude_pattern:
        original_count = len(rows)
        rows = [
            r for r in rows
            if not any(pat.lower() in (r["original_filename"] or "").lower() for pat in args.exclude_pattern)
        ]
        skipped = original_count - len(rows)
        if skipped:
            print(f"Excluded {skipped} file(s) matching --exclude-pattern {args.exclude_pattern}.")

    if not rows:
        print(f"No SQL processing rows found for {date_label}.")
        return

    print(f"Found {len(rows)} SQL processing rows for {date_label}. Checking SQL API status...")

    approved_count = 0
    skipped_count = 0
    failed_count = 0
    remaining_items: List[Dict] = []

    form_data = {
        "source": args.source.strip(),
        "source_url": args.source_url.strip(),
        "released_on": args.released_on.strip(),
        "updated_on": args.updated_on.strip(),
    }
    if args.business_metadata.strip():
        form_data["business_metadata"] = args.business_metadata.strip()

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        for row in rows:
            file_id = row["id"]
            filename = row["original_filename"]
            err = row["error_message"] or ""
            job_id = err.replace("sql_job_id:", "", 1)

            # Check current SQL status
            try:
                status_code, payload = await get_sql_status(client, sql_api_url, job_id)
            except Exception as ex:
                print(f"[WARN] {filename} ({file_id}) job={job_id}: SQL API unreachable: {ex}")
                skipped_count += 1
                continue

            if status_code != 200:
                print(f"[SKIP] {filename} ({file_id}) job={job_id} http={status_code} (not approving)")
                skipped_count += 1
                continue

            sql_status = payload.get("status", "")

            # Already terminal — just sync
            if sql_status in TERMINAL_SUCCESS:
                if not dry_run:
                    await mark_done(conn, file_id)
                approved_count += 1
                prefix = "[DRY-SYNC-DONE]" if dry_run else "[SYNC-DONE]"
                print(f"{prefix} {filename} ({file_id}) job={job_id} already={sql_status}")
                continue

            if sql_status in TERMINAL_FAILURE:
                if not dry_run:
                    await mark_failed(conn, file_id, f"SQL job {sql_status}")
                failed_count += 1
                prefix = "[DRY-SYNC-FAIL]" if dry_run else "[SYNC-FAIL]"
                print(f"{prefix} {filename} ({file_id}) job={job_id} already={sql_status}")
                continue

            if sql_status != "awaiting_approval":
                print(f"[SKIP] {filename} ({file_id}) job={job_id} sql_status={sql_status} (not awaiting approval)")
                skipped_count += 1
                remaining_items.append({
                    "file_id": file_id, "filename": filename, "job_id": job_id,
                    "sql_status": sql_status, "has_source": False, "has_source_url": False,
                    "has_released_on": False, "has_updated_on": False, "has_business_metadata": False,
                    "source_identifier": row.get("source_identifier"),
                    "inbound_source_url": row.get("source_url"),
                })
                continue

            # Approve this job
            if dry_run:
                print(f"[DRY-APPROVE] {filename} ({file_id}) job={job_id}")
                skipped_count += 1
                remaining_items.append({
                    "file_id": file_id, "filename": filename, "job_id": job_id,
                    "sql_status": "would_approve", "has_source": bool(args.source.strip()),
                    "has_source_url": bool(args.source_url.strip()),
                    "has_released_on": bool(args.released_on.strip()),
                    "has_updated_on": bool(args.updated_on.strip()),
                    "has_business_metadata": bool(args.business_metadata.strip()),
                    "source_identifier": row.get("source_identifier"),
                    "inbound_source_url": row.get("source_url"),
                })
                continue

            approve_resp = await client.post(
                f"{sql_api_url.rstrip('/')}/approve/{job_id}",
                data=form_data,
            )
            if approve_resp.status_code != 200:
                failed_count += 1
                print(f"[APPROVE-FAIL] {filename} ({file_id}) job={job_id} http={approve_resp.status_code} {approve_resp.text[:200]}")
                remaining_items.append({
                    "file_id": file_id, "filename": filename, "job_id": job_id,
                    "sql_status": f"approve_http_{approve_resp.status_code}", "has_source": False,
                    "has_source_url": False, "has_released_on": False, "has_updated_on": False,
                    "has_business_metadata": False, "source_identifier": row.get("source_identifier"),
                    "inbound_source_url": row.get("source_url"),
                })
                continue

            approved_count += 1
            print(f"[APPROVED] {filename} ({file_id}) job={job_id} — polling for completion...")

            # Poll for completion
            final_status = "approved"
            for _ in range(max(1, args.poll_attempts)):
                await asyncio.sleep(max(1, args.poll_interval))
                try:
                    st_resp = await client.get(f"{sql_api_url.rstrip('/')}/status/{job_id}")
                    if st_resp.status_code == 200:
                        final_status = (st_resp.json() or {}).get("status", final_status)
                        if final_status in TERMINAL_SUCCESS.union(TERMINAL_FAILURE):
                            break
                except Exception:
                    break

            # Sync to our DB
            from app.services.sql_adapter import check_sql_job_completion
            await check_sql_job_completion(file_id)
            print(f"  -> synced (final_sql_status={final_status})")

    print("\nSummary (approve-individual)")
    print(f"  mode:     {'dry-run' if dry_run else 'apply'}")
    print(f"  approved: {approved_count}")
    print(f"  failed:   {failed_count}")
    print(f"  skipped:  {skipped_count}")

    _print_remaining(remaining_items)


async def main() -> None:
    args = parse_args()
    if args.for_date and args.all_dates:
        raise SystemExit("Use either --for-date or --all-dates, not both")

    if args.all_dates:
        processing_date = None
        date_label = "all dates"
    elif args.for_date:
        try:
            processing_date = datetime.strptime(args.for_date.strip(), "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("Invalid --for-date. Use YYYY-MM-DD.")
        date_label = processing_date.isoformat()
    else:
        processing_date = date.today()
        date_label = processing_date.isoformat()

    dry_run = args.dry_run and not args.apply
    if args.dry_run and args.apply:
        raise SystemExit("Use only one of --dry-run or --apply")

    if args.mark_missing_done and args.mark_missing_failed:
        raise SystemExit("Use only one of --mark-missing-done or --mark-missing-failed")

    if args.reingest_missing and (args.mark_missing_done or args.mark_missing_failed):
        raise SystemExit("--reingest-missing cannot be combined with --mark-missing-done/--mark-missing-failed")

    if args.reingest_missing:
        required = [args.source.strip(), args.source_url.strip(), args.released_on.strip(), args.updated_on.strip()]
        if not all(required):
            raise SystemExit("--reingest-missing requires --source, --source-url, --released-on, --updated-on")
        if dry_run:
            print("[INFO] dry-run enabled: reingest actions will be planned but not executed.")

    if args.retry_failed:
        required = [args.source.strip(), args.source_url.strip(), args.released_on.strip(), args.updated_on.strip()]
        if not all(required):
            raise SystemExit("--retry-failed requires --source, --source-url, --released-on, --updated-on")
        if dry_run:
            print("[INFO] dry-run enabled: retry-failed actions will be planned but not executed.")

    if args.retry_sql_upload_errors and dry_run:
        print("[INFO] dry-run enabled: upload-retry actions will be planned but not executed.")

    if args.approve_individual:
        required = [args.source.strip(), args.source_url.strip(), args.released_on.strip(), args.updated_on.strip()]
        if not all(required):
            raise SystemExit("--approve-individual requires --source, --source-url, --released-on, --updated-on")
        if dry_run:
            print("[INFO] dry-run enabled: individual-approve actions will be planned but not executed.")

    database_url = (
        getattr(app_settings, "DATABASE_URL", "") if app_settings else os.getenv("DATABASE_URL", "")
    )
    sql_api_url = (
        getattr(app_settings, "SQL_PIPELINE_API_URL", "http://localhost:8000")
        if app_settings
        else os.getenv("SQL_PIPELINE_API_URL", "http://localhost:8000")
    )

    if not database_url:
        raise SystemExit("DATABASE_URL is required")

    app_db_pool_started = False
    if args.reingest_missing or args.retry_failed or args.retry_sql_upload_errors or args.approve_individual:
        if connect_db is None or disconnect_db is None:
            raise SystemExit("App DB helpers unavailable; cannot run this mode in standalone")
        await connect_db()
        app_db_pool_started = True

    conn = await asyncpg.connect(database_url)
    try:
        # ── Retry SQL upload errors: re-upload files that never got a job_id ──
        if args.retry_sql_upload_errors:
            await _run_retry_sql_upload_errors(
                conn=conn,
                args=args,
                processing_date=processing_date,
                date_label=date_label,
                dry_run=dry_run,
            )
            return

        # ── Approve individual: approve awaiting_approval jobs one-by-one ──
        if args.approve_individual:
            await _run_approve_individual(
                conn=conn,
                args=args,
                processing_date=processing_date,
                date_label=date_label,
                sql_api_url=sql_api_url,
                dry_run=dry_run,
            )
            return

        # ── Retry-failed mode runs against 'failed' rows, not 'processing' ──
        if args.retry_failed:
            await _run_retry_failed(
                conn=conn,
                args=args,
                processing_date=processing_date,
                sql_api_url=sql_api_url,
                dry_run=dry_run,
            )
            return

        rows = await fetch_processing_rows(conn, processing_date)
        if not rows:
            print(f"No SQL processing rows found for {date_label}.")
            return

        # Apply --exclude-pattern filters
        if args.exclude_pattern:
            original_count = len(rows)
            rows = [
                r for r in rows
                if not any(pat.lower() in (r["original_filename"] or "").lower() for pat in args.exclude_pattern)
            ]
            skipped = original_count - len(rows)
            if skipped:
                print(f"Excluded {skipped} file(s) matching --exclude-pattern {args.exclude_pattern}.")
            if not rows:
                print("No rows remaining after exclusion filter.")
                return

        print(f"Found {len(rows)} SQL rows in processing for {date_label}.")

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
                        # When --reingest-missing is active, re-upload failed jobs
                        # instead of just syncing the failure to our DB.
                        if args.reingest_missing:
                            if dry_run:
                                pending_count += 1
                                print(
                                    f"[DRY-REINGEST-FAILED] {filename} ({file_id}) old_job={job_id} "
                                    f"sql_status={sql_status} source={args.source} source_url={args.source_url}"
                                )
                                remaining_items.append(
                                    {
                                        "file_id": file_id,
                                        "filename": filename,
                                        "job_id": job_id,
                                        "sql_status": "would_reingest_failed_job",
                                        "has_source": bool(args.source.strip()),
                                        "has_source_url": bool(args.source_url.strip()),
                                        "has_released_on": bool(args.released_on.strip()),
                                        "has_updated_on": bool(args.updated_on.strip()),
                                        "has_business_metadata": bool(args.business_metadata.strip()),
                                        "source_identifier": row.get("source_identifier"),
                                        "inbound_source_url": row.get("source_url"),
                                    }
                                )
                            else:
                                outcome, msg = await _reingest_and_complete(
                                    file_id=file_id,
                                    filename=filename,
                                    sql_api_url=sql_api_url,
                                    source=args.source.strip(),
                                    source_url=args.source_url.strip(),
                                    released_on=args.released_on.strip(),
                                    updated_on=args.updated_on.strip(),
                                    business_metadata=args.business_metadata,
                                    poll_attempts=args.poll_attempts,
                                    poll_interval=args.poll_interval,
                                )
                                if outcome == "done":
                                    done_count += 1
                                    print(f"[REINGEST-DONE] {filename} ({file_id}) old_job={job_id} {msg}")
                                elif outcome == "failed":
                                    failed_count += 1
                                    print(f"[REINGEST-FAIL] {filename} ({file_id}) old_job={job_id} {msg}")
                                else:
                                    pending_count += 1
                                    print(f"[REINGEST-PEND] {filename} ({file_id}) old_job={job_id} {msg}")
                        else:
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
                    if args.reingest_missing:
                        if dry_run:
                            pending_count += 1
                            print(
                                f"[DRY-REINGEST] {filename} ({file_id}) old_job={job_id} "
                                f"source={args.source} source_url={args.source_url}"
                            )
                            remaining_items.append(
                                {
                                    "file_id": file_id,
                                    "filename": filename,
                                    "job_id": job_id,
                                    "sql_status": "would_reingest_missing_job",
                                    "has_source": bool(args.source.strip()),
                                    "has_source_url": bool(args.source_url.strip()),
                                    "has_released_on": bool(args.released_on.strip()),
                                    "has_updated_on": bool(args.updated_on.strip()),
                                    "has_business_metadata": bool(args.business_metadata.strip()),
                                    "source_identifier": row.get("source_identifier"),
                                    "inbound_source_url": row.get("source_url"),
                                }
                            )
                        else:
                            outcome, msg = await _reingest_and_complete(
                                file_id=file_id,
                                filename=filename,
                                sql_api_url=sql_api_url,
                                source=args.source.strip(),
                                source_url=args.source_url.strip(),
                                released_on=args.released_on.strip(),
                                updated_on=args.updated_on.strip(),
                                business_metadata=args.business_metadata,
                                poll_attempts=args.poll_attempts,
                                poll_interval=args.poll_interval,
                            )
                            if outcome == "done":
                                done_count += 1
                                print(f"[REINGEST-DONE] {filename} ({file_id}) old_job={job_id} {msg}")
                            elif outcome == "failed":
                                failed_count += 1
                                print(f"[REINGEST-FAIL] {filename} ({file_id}) old_job={job_id} {msg}")
                            else:
                                pending_count += 1
                                print(f"[REINGEST-PEND] {filename} ({file_id}) old_job={job_id} {msg}")
                                remaining_items.append(
                                    {
                                        "file_id": file_id,
                                        "filename": filename,
                                        "job_id": job_id,
                                        "sql_status": "reingested_pending",
                                        "has_source": bool(args.source.strip()),
                                        "has_source_url": bool(args.source_url.strip()),
                                        "has_released_on": bool(args.released_on.strip()),
                                        "has_updated_on": bool(args.updated_on.strip()),
                                        "has_business_metadata": bool(args.business_metadata.strip()),
                                        "source_identifier": row.get("source_identifier"),
                                        "inbound_source_url": row.get("source_url"),
                                    }
                                )
                    elif args.mark_missing_done:
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
        if app_db_pool_started:
            await disconnect_db()


if __name__ == "__main__":
    asyncio.run(main())
