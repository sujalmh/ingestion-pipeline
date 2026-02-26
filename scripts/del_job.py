import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

root = Path(__file__).resolve().parent.parent
env_path = root / ".env"
load_dotenv(env_path)

DATABASE_URL = os.getenv("DATABASE_URL")

async def main():
    import asyncpg
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Check by stored_filename
        records = await conn.fetch(
            "SELECT id::text, stored_filename, original_filename FROM inbound_files_metadata WHERE stored_filename LIKE '%e5b806d9-c4a7-4173-a287-2b91705572e8%'"
        )
        print("Found by stored_filename:")
        for r in records:
            print(dict(r))
            deleted = await conn.fetch(
                "DELETE FROM inbound_files_metadata WHERE id = $1 RETURNING id::text",
                r['id']
            )
            print(f"Deleted ID: {deleted[0]['id']}")
            
        # Check by original_filename directly
        records2 = await conn.fetch(
            "SELECT id::text, stored_filename, original_filename FROM inbound_files_metadata WHERE original_filename LIKE '%Corporate_Sector_Finances_of_FDI_Companies_Financial_Ratios%'"
        )
        print("Found by original_filename:")
        for r in records2:
            print(dict(r))
            deleted = await conn.fetch(
                "DELETE FROM inbound_files_metadata WHERE id = $1 RETURNING id::text",
                r['id']
            )
            print(f"Deleted ID: {deleted[0]['id']}")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
