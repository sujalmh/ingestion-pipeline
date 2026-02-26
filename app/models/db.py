"""
Database operations for Automated Ingestion Pipeline

Tables:
- inbound_files_metadata (NEW): Tracks file lifecycle through the pipeline
- operational_metadata (EXISTING): Stores business metadata for all files
"""
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
import asyncpg
from dotenv import load_dotenv

load_dotenv()

# Global connection pool
_pool: Optional[asyncpg.Pool] = None


# ==========================================
# Connection Management
# ==========================================

async def get_db_pool() -> asyncpg.Pool:
    """Returns the database connection pool."""
    global _pool
    if _pool is None:
        raise Exception("Database connection pool not initialized. Call connect_db() first.")
    return _pool


async def connect_db():
    """Initialize database connection pool."""
    global _pool
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set.")
    
    try:
        _pool = await asyncpg.create_pool(dsn=db_url)
        print("✅ Database connection pool created successfully.")
    except asyncpg.InvalidCatalogNameError:
        print("⚠️ Database not found. Creating it...")
        from urllib.parse import urlparse
        parsed = urlparse(db_url)
        db_user = parsed.username
        db_pass = parsed.password or ""
        db_host = parsed.hostname
        db_port = parsed.port or 5432
        db_name = parsed.path.lstrip("/")
        
        admin_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/postgres"
        conn = await asyncpg.connect(admin_url)
        await conn.execute(f'CREATE DATABASE "{db_name}" OWNER "{db_user}";')
        await conn.close()
        
        _pool = await asyncpg.create_pool(dsn=db_url)
        print("✅ Database created and connection pool initialized.")


async def disconnect_db():
    """Close database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        print("✅ Database connection pool closed.")


# ==========================================
# Table Initialization
# ==========================================

async def init_inbound_files_table():
    """
    Create the inbound_files_metadata table if it doesn't exist.
    This table tracks file lifecycle through the automated ingestion pipeline.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Create the table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS inbound_files_metadata (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                
                -- Source tracking
                source_type VARCHAR(50) NOT NULL,
                source_identifier VARCHAR(255),
                source_url TEXT,
                
                -- File info
                original_filename VARCHAR(500),
                stored_filename VARCHAR(500),
                file_path TEXT,
                file_size BIGINT,
                file_extension VARCHAR(20),
                mime_type VARCHAR(100),
                sha256_hash CHAR(64) UNIQUE,
                
                -- Classification results
                classification VARCHAR(20),
                classification_confidence FLOAT,
                routed_to VARCHAR(50),
                
                -- References to downstream processing
                operational_metadata_id INT,
                extracted_table_ids INT[],
                vector_file_id UUID,
                
                -- Status tracking
                status VARCHAR(30) NOT NULL DEFAULT 'open_request',
                error_message TEXT,
                retry_count INT DEFAULT 0,
                
                -- Timestamps
                created_at TIMESTAMPTZ DEFAULT now(),
                classification_completed_at TIMESTAMPTZ,
                processing_started_at TIMESTAMPTZ,
                processing_completed_at TIMESTAMPTZ
            );
        """)
        
        # Create indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_inbound_status 
            ON inbound_files_metadata(status);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_inbound_source 
            ON inbound_files_metadata(source_type);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_inbound_hash 
            ON inbound_files_metadata(sha256_hash);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_inbound_created 
            ON inbound_files_metadata(created_at);
        """)
        
        print("✅ inbound_files_metadata table initialized.")


async def init_operational_metadata_table():
    """
    Verify operational_metadata table exists.
    This table already exists on the dev database - no creation needed.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Just verify the table exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'operational_metadata'
            );
        """)
        if exists:
            print("✅ operational_metadata table exists.")
        else:
            raise Exception("operational_metadata table not found! This table must exist on the database.")


async def init_all_tables():
    """Initialize all required tables."""
    await init_inbound_files_table()
    await init_operational_metadata_table()
    # Import here to avoid circular import
    from app.services.hitl import init_hitl_table
    await init_hitl_table()


# ==========================================
# Inbound Files CRUD Operations
# ==========================================

async def insert_inbound_file(
    source_type: str,
    original_filename: str,
    stored_filename: str,
    file_path: str,
    file_size: int,
    file_extension: str,
    sha256_hash: str,
    source_identifier: Optional[str] = None,
    source_url: Optional[str] = None,
    mime_type: Optional[str] = None,
) -> str:
    """
    Insert a new inbound file record.
    
    Returns:
        The UUID of the newly created record.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        file_id = await conn.fetchval(
            """
            INSERT INTO inbound_files_metadata (
                source_type, source_identifier, source_url,
                original_filename, stored_filename, file_path,
                file_size, file_extension, mime_type, sha256_hash,
                status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id::text;
            """,
            source_type,
            source_identifier,
            source_url,
            original_filename,
            stored_filename,
            file_path,
            file_size,
            file_extension,
            mime_type,
            sha256_hash,
            "open_request"
        )
        return file_id


_UNSET = object()  # Sentinel to distinguish "not provided" from "set to None"


async def update_inbound_status(
    file_id: str,
    status: str,
    error_message=_UNSET,
    classification: Optional[str] = None,
    classification_confidence: Optional[float] = None,
    routed_to: Optional[str] = None,
    operational_metadata_id: Optional[int] = None,
    extracted_table_ids: Optional[List[int]] = None,
    vector_file_id: Optional[str] = None,
) -> bool:
    """
    Update the status and other fields of an inbound file.
    
    Pass error_message=None to explicitly clear it (set to NULL in DB).
    Omit error_message entirely to leave it unchanged.
    
    Returns:
        True if update was successful, False otherwise.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Build dynamic update query
        updates = ["status = $2"]
        params = [file_id, status]
        param_count = 2
        
        if error_message is not _UNSET:
            param_count += 1
            updates.append(f"error_message = ${param_count}")
            params.append(error_message)
        
        if classification is not None:
            param_count += 1
            updates.append(f"classification = ${param_count}")
            params.append(classification)
            
        if classification_confidence is not None:
            param_count += 1
            updates.append(f"classification_confidence = ${param_count}")
            params.append(classification_confidence)
            
        if routed_to is not None:
            param_count += 1
            updates.append(f"routed_to = ${param_count}")
            params.append(routed_to)
            
        if operational_metadata_id is not None:
            param_count += 1
            updates.append(f"operational_metadata_id = ${param_count}")
            params.append(operational_metadata_id)
            
        if extracted_table_ids is not None:
            param_count += 1
            updates.append(f"extracted_table_ids = ${param_count}")
            params.append(extracted_table_ids)
            
        if vector_file_id is not None:
            param_count += 1
            updates.append(f"vector_file_id = ${param_count}::uuid")
            params.append(vector_file_id)
        
        # Add timestamp updates based on status
        if status == "classified":
            updates.append("classification_completed_at = now()")
        elif status == "processing":
            updates.append("processing_started_at = now()")
        elif status in ("done", "failed"):
            updates.append("processing_completed_at = now()")
        
        query = f"""
            UPDATE inbound_files_metadata
            SET {', '.join(updates)}
            WHERE id = $1::uuid;
        """
        
        result = await conn.execute(query, *params)
        return result == "UPDATE 1"


async def update_inbound_retry_count(file_id: str) -> int:
    """
    Increment the retry count for a file.
    
    Returns:
        The new retry count.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        new_count = await conn.fetchval(
            """
            UPDATE inbound_files_metadata
            SET retry_count = retry_count + 1,
                status = 'open_request',
                error_message = NULL
            WHERE id = $1::uuid
            RETURNING retry_count;
            """,
            file_id
        )
        return new_count


async def get_inbound_file(file_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single inbound file by ID.
    
    Returns:
        File record as dictionary, or None if not found.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            """
            SELECT 
                id::text, source_type, source_identifier, source_url,
                original_filename, stored_filename, file_path,
                file_size, file_extension, mime_type, sha256_hash,
                classification, classification_confidence, routed_to,
                operational_metadata_id, extracted_table_ids, vector_file_id::text,
                status, error_message, retry_count,
                created_at, classification_completed_at,
                processing_started_at, processing_completed_at
            FROM inbound_files_metadata
            WHERE id = $1::uuid;
            """,
            file_id
        )
        return dict(record) if record else None


async def get_inbound_files(
    status: Optional[str] = None,
    source_type: Optional[str] = None,
    routed_to: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Get multiple inbound files with optional filtering.

    Returns:
        List of file records.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Build query with optional filters
        conditions = []
        params = []
        param_count = 0

        if status:
            param_count += 1
            conditions.append(f"status = ${param_count}")
            params.append(status)

        if source_type:
            param_count += 1
            conditions.append(f"source_type = ${param_count}")
            params.append(source_type)

        if routed_to:
            param_count += 1
            conditions.append(f"routed_to = ${param_count}")
            params.append(routed_to)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        param_count += 1
        limit_param = param_count
        param_count += 1
        offset_param = param_count
        params.extend([limit, offset])
        
        query = f"""
            SELECT 
                id::text, source_type, source_identifier, source_url,
                original_filename, stored_filename, file_path,
                file_size, file_extension, mime_type, sha256_hash,
                classification, classification_confidence, routed_to,
                operational_metadata_id, extracted_table_ids, vector_file_id::text,
                status, error_message, retry_count,
                created_at, classification_completed_at,
                processing_started_at, processing_completed_at
            FROM inbound_files_metadata
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${limit_param} OFFSET ${offset_param};
        """
        
        records = await conn.fetch(query, *params)
        return [dict(r) for r in records]


async def check_duplicate_hash(sha256_hash: str) -> Optional[Dict[str, Any]]:
    """
    Check if a file with the given hash already exists.
    
    Returns:
        Existing file record if duplicate found, None otherwise.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            """
            SELECT id::text, original_filename, status, created_at
            FROM inbound_files_metadata
            WHERE sha256_hash = $1;
            """,
            sha256_hash
        )
        return dict(record) if record else None


# ==========================================
# Operational Metadata Operations
# ==========================================

async def insert_operational_metadata(
    table_name: str,
    source_url: Optional[str] = None,
    major_domain: Optional[str] = None,
    sub_domain: Optional[str] = None,
    brief_summary: Optional[str] = None,
    rows_count: Optional[int] = None,
    columns: Optional[str] = None,
    period_cols: Optional[str] = None,
    first_available_value: Optional[str] = None,
    last_available_value: Optional[str] = None,
    business_metadata: Optional[str] = None,
    table_view: Optional[str] = None,
) -> int:
    """
    Insert a new operational metadata entry.
    
    This is called after classification to store the base-level metadata
    (domain, subdomain, summary) for every file.
    
    Returns:
        The ID of the newly created record.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record_id = await conn.fetchval(
            """
            INSERT INTO operational_metadata (
                table_name, table_view, source_url,
                major_domain, sub_domain, brief_summary,
                rows_count, columns, period_cols,
                first_available_value, last_available_value,
                business_metadata, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id;
            """,
            table_name,
            table_view,
            source_url,
            major_domain,
            sub_domain,
            brief_summary,
            rows_count,
            columns,
            period_cols,
            first_available_value,
            last_available_value,
            business_metadata,
        )
        return record_id


async def update_operational_metadata(
    record_id: int,
    major_domain: Optional[str] = None,
    sub_domain: Optional[str] = None,
    brief_summary: Optional[str] = None,
    rows_count: Optional[int] = None,
    columns: Optional[str] = None,
    period_cols: Optional[str] = None,
    first_available_value: Optional[str] = None,
    last_available_value: Optional[str] = None,
    business_metadata: Optional[str] = None,
) -> bool:
    """
    Update an existing operational metadata entry.
    
    Returns:
        True if update was successful, False otherwise.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        # Build dynamic update query
        updates = ["updated_at = CURRENT_TIMESTAMP"]
        params = [record_id]
        param_count = 1
        
        if major_domain is not None:
            param_count += 1
            updates.append(f"major_domain = ${param_count}")
            params.append(major_domain)
            
        if sub_domain is not None:
            param_count += 1
            updates.append(f"sub_domain = ${param_count}")
            params.append(sub_domain)
            
        if brief_summary is not None:
            param_count += 1
            updates.append(f"brief_summary = ${param_count}")
            params.append(brief_summary)
            
        if rows_count is not None:
            param_count += 1
            updates.append(f"rows_count = ${param_count}")
            params.append(rows_count)
            
        if columns is not None:
            param_count += 1
            updates.append(f"columns = ${param_count}")
            params.append(columns)
            
        if period_cols is not None:
            param_count += 1
            updates.append(f"period_cols = ${param_count}")
            params.append(period_cols)
            
        if first_available_value is not None:
            param_count += 1
            updates.append(f"first_available_value = ${param_count}")
            params.append(first_available_value)
            
        if last_available_value is not None:
            param_count += 1
            updates.append(f"last_available_value = ${param_count}")
            params.append(last_available_value)
            
        if business_metadata is not None:
            param_count += 1
            updates.append(f"business_metadata = ${param_count}")
            params.append(business_metadata)
        
        query = f"""
            UPDATE operational_metadata
            SET {', '.join(updates)}
            WHERE id = $1;
        """
        
        result = await conn.execute(query, *params)
        return result == "UPDATE 1"


async def get_operational_metadata(record_id: int) -> Optional[Dict[str, Any]]:
    """
    Get an operational metadata record by ID.
    
    Returns:
        Record as dictionary, or None if not found.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            """
            SELECT *
            FROM operational_metadata
            WHERE id = $1;
            """,
            record_id
        )
        return dict(record) if record else None


async def check_operational_metadata_exists(table_name: str) -> Optional[int]:
    """
    Check if an operational metadata entry exists for a table name.
    
    Returns:
        The record ID if exists, None otherwise.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        record_id = await conn.fetchval(
            """
            SELECT id
            FROM operational_metadata
            WHERE table_name = $1;
            """,
            table_name
        )
        return record_id


async def exists_completed_sql_load_for_table(table_name: str) -> bool:
    """
    Return True if there is at least one completed SQL load for this exact
    table name. Used to route first load as OTL, subsequent loads as INC.
    Status 'done' is set by sql_adapter when the SQL pipeline reports
    completed/incremental_load_completed.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM inbound_files_metadata m
                JOIN operational_metadata o ON m.operational_metadata_id = o.id
                WHERE m.routed_to IN ('sql_otl', 'sql_inc')
                  AND m.status IN ('completed', 'incremental_load_completed', 'done')
                  AND o.table_name = $1
            );
            """,
            table_name,
        )
        return bool(exists)
