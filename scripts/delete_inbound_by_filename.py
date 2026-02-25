"""
Delete ingestion record(s) by original_filename from inbound_files_metadata.
Usage: python scripts/delete_inbound_by_filename.py <original_filename>
Example: python scripts/delete_inbound_by_filename.py ethanol-blending-with-petrol.xlsx
"""
import asyncio
import os
import sys
from pathlib import Path

# Load .env from project root
root = Path(__file__).resolve().parent.parent
env_path = root / ".env"
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set. Set it in .env or environment.")
    sys.exit(1)


async def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/delete_inbound_by_filename.py <original_filename>")
        print("Example: python scripts/delete_inbound_by_filename.py ethanol-blending-with-petrol.xlsx")
        sys.exit(1)
    filename = sys.argv[1]

    import asyncpg
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        deleted = await conn.fetch(
            "DELETE FROM inbound_files_metadata WHERE original_filename = $1 RETURNING id::text",
            filename,
        )
        if not deleted:
            print(f"No record found for original_filename '{filename}'")
            sys.exit(1)
        print(f"Deleted {len(deleted)} record(s) for '{filename}' (id(s): {[r['id'] for r in deleted]})")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
