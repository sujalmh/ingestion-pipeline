"""
Combined cleanup for ingestion orchestrator DB and files:

1. Delete physical files (under UPLOAD_DIRECTORY) referenced in inbound_files_metadata
2. Drop all DB tables that are not required by the orchestrator
3. Truncate required tables: hitl_queue, inbound_files_metadata, operational_metadata, tables_metadata
4. Optionally DROP the pipeline tables (--drop-tables)

Required tables (kept and truncated): inbound_files_metadata, operational_metadata,
  hitl_queue, tables_metadata. All other tables in public schema are dropped.

Run from ingestion-pipeline directory:
  python run_cleanup_ingestion.py
  python run_cleanup_ingestion.py --no-files
  python run_cleanup_ingestion.py --drop-tables
  python run_cleanup_ingestion.py --include-tables-metadata
  python run_cleanup_ingestion.py --dry-run
"""
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from app.config import settings
from app.models.db import connect_db, disconnect_db, get_db_pool

# Tables required by the ingestion orchestrator (keep these; all others are dropped)
REQUIRED_TABLES = {
    "inbound_files_metadata",
    "operational_metadata",
    "hitl_queue",
    "tables_metadata",
}

TRUNCATE_ORDER = [
    "hitl_queue",
    "inbound_files_metadata",
    "operational_metadata",
    "tables_metadata",
]


async def delete_physical_files(pool, dry_run: bool) -> int:
    """Delete files on disk referenced in inbound_files_metadata. Returns count deleted."""
    async with pool.acquire() as conn:
        # Table may not exist if --drop-tables was used previously
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'inbound_files_metadata'
            );
            """
        )
        if not exists:
            return 0
        rows = await conn.fetch(
            "SELECT id::text, file_path FROM inbound_files_metadata WHERE file_path IS NOT NULL AND file_path != '';"
        )
    deleted = 0
    for row in rows:
        path = Path(row["file_path"])
        if path.is_file():
            if dry_run:
                print(f"  [dry-run] would delete {path}")
            else:
                try:
                    path.unlink()
                    print(f"  Deleted file: {path}")
                except Exception as e:
                    print(f"  Failed to delete {path}: {e}")
            deleted += 1
    return deleted


async def get_public_tables(pool) -> list:
    """Return list of table names in public schema."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
    return [r["tablename"] for r in rows]


async def drop_non_required_tables(pool, dry_run: bool) -> int:
    """Drop all tables not in REQUIRED_TABLES. Returns number dropped."""
    tables = await get_public_tables(pool)
    to_drop = [t for t in tables if t not in REQUIRED_TABLES]
    if not to_drop:
        return 0
    async with pool.acquire() as conn:
        for t in to_drop:
            if dry_run:
                print(f"  [dry-run] would DROP TABLE IF EXISTS {t} CASCADE;")
            else:
                await conn.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE;')
                print(f"  Dropped {t}")
    return len(to_drop)


async def truncate_required_tables(pool, include_tables_metadata: bool, dry_run: bool) -> None:
    """Truncate required tables in safe order (respecting FKs)."""
    tables = await get_public_tables(pool)
    to_truncate = [t for t in TRUNCATE_ORDER if t in tables]
    if not include_tables_metadata:
        to_truncate = [t for t in to_truncate if t != "tables_metadata"]
    if not to_truncate:
        return
    async with pool.acquire() as conn:
        for t in to_truncate:
            if dry_run:
                print(f"  [dry-run] would TRUNCATE TABLE {t} CASCADE;")
            else:
                await conn.execute(f'TRUNCATE TABLE "{t}" CASCADE;')
                print(f"  Truncated {t}")


async def drop_pipeline_tables(pool, dry_run: bool) -> None:
    """DROP hitl_queue, inbound_files_metadata, operational_metadata."""
    async with pool.acquire() as conn:
        for t in ("hitl_queue", "inbound_files_metadata", "operational_metadata"):
            if dry_run:
                print(f"  [dry-run] would DROP TABLE IF EXISTS {t} CASCADE;")
            else:
                await conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")
                print(f"  Dropped {t}")


async def run(
    delete_files: bool = True,
    include_tables_metadata: bool = False,
    drop_tables: bool = False,
    dry_run: bool = False,
):
    if not settings.DATABASE_URL:
        print("ERROR: DATABASE_URL not set. Set it in .env")
        sys.exit(1)

    if dry_run:
        print("DRY RUN - no changes will be made\n")

    print("Connecting to database...")
    await connect_db()
    pool = await get_db_pool()

    try:
        if delete_files:
            print("\nDeleting physical ingested files...")
            n = await delete_physical_files(pool, dry_run)
            print(f"  Deleted {n} file(s).")
        else:
            print("\nSkipping physical files (--no-files).")

        tables = await get_public_tables(pool)
        print(f"\nFound {len(tables)} table(s) in public schema.")

        to_drop = [t for t in tables if t not in REQUIRED_TABLES]
        if to_drop:
            print(f"Dropping {len(to_drop)} non-required table(s)...")
            await drop_non_required_tables(pool, dry_run)
        else:
            print("No non-required tables to drop.")

        tables_after = await get_public_tables(pool) if not dry_run else [t for t in tables if t in REQUIRED_TABLES]
        to_truncate_list = [
            t for t in TRUNCATE_ORDER
            if t in tables_after and (include_tables_metadata or t != "tables_metadata")
        ]
        if to_truncate_list:
            print(f"\nTruncating required table(s): {to_truncate_list}...")
            await truncate_required_tables(pool, include_tables_metadata, dry_run)

        if drop_tables:
            print("\nDropping pipeline tables...")
            await drop_pipeline_tables(pool, dry_run)

        print("\nCleanup finished.")
    finally:
        await disconnect_db()


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Delete ingested files, drop non-required DB tables, truncate required tables, optionally drop pipeline tables."
    )
    parser.add_argument("--no-files", action="store_true", help="Do not delete physical files from disk")
    parser.add_argument("--include-tables-metadata", action="store_true", help="Include tables_metadata in truncate (default: only hitl, inbound, operational_metadata)")
    parser.add_argument("--drop-tables", action="store_true", help="DROP hitl_queue, inbound_files_metadata, operational_metadata")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be done")
    args = parser.parse_args()

    asyncio.run(run(
        delete_files=not args.no_files,
        include_tables_metadata=args.include_tables_metadata,
        drop_tables=args.drop_tables,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
