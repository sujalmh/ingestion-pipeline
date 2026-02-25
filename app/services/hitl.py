"""
Human-In-The-Loop (HITL) Service

Handles files that require human review due to:
1. Low classification confidence
2. Unknown file types
3. Processing failures that need manual intervention

The HITL queue allows reviewers to:
- View pending items
- Approve classifications (with optional override)
- Reject files
- Add notes for future reference
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

from app.models.db import get_db_pool, update_inbound_status, get_inbound_file
from app.models.schemas import FileStatus, Classification, RoutingDecision


class HITLStatus(str, Enum):
    """Status values for HITL queue items."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    REASSIGNED = "reassigned"


class HITLReason(str, Enum):
    """Reasons for HITL escalation."""
    LOW_CONFIDENCE = "low_confidence"
    UNKNOWN_TYPE = "unknown_type"
    PROCESSING_FAILED = "processing_failed"
    AMBIGUOUS_CLASSIFICATION = "ambiguous_classification"
    MANUAL_REVIEW_REQUESTED = "manual_review_requested"


# ==========================================
# HITL Table Management
# ==========================================

async def init_hitl_table():
    """
    Create the hitl_queue table if it doesn't exist.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS hitl_queue (
                id SERIAL PRIMARY KEY,
                inbound_file_id UUID NOT NULL,
                reason VARCHAR(50) NOT NULL,
                reason_details TEXT,
                original_classification VARCHAR(20),
                original_confidence FLOAT,
                suggested_classification VARCHAR(20),
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                reviewer_id VARCHAR(100),
                reviewer_notes TEXT,
                final_classification VARCHAR(20),
                final_routing VARCHAR(50),
                created_at TIMESTAMPTZ DEFAULT now(),
                reviewed_at TIMESTAMPTZ,
                CONSTRAINT fk_inbound_file
                    FOREIGN KEY (inbound_file_id)
                    REFERENCES inbound_files_metadata(id)
                    ON DELETE CASCADE
            );
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hitl_status
            ON hitl_queue(status);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_hitl_file
            ON hitl_queue(inbound_file_id);
        """)

        print("✅ hitl_queue table initialized.")


# ==========================================
# HITL Queue Operations
# ==========================================

async def escalate_to_hitl(
    file_id: str,
    reason: HITLReason,
    reason_details: Optional[str] = None,
    original_classification: Optional[str] = None,
    original_confidence: Optional[float] = None,
) -> int:
    """
    Escalate a file to the HITL queue.

    Returns:
        The ID of the created HITL queue entry.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        hitl_id = await conn.fetchval(
            """
            INSERT INTO hitl_queue (
                inbound_file_id, reason, reason_details,
                original_classification, original_confidence, status
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id;
            """,
            file_id,
            reason.value,
            reason_details,
            original_classification,
            original_confidence,
            HITLStatus.PENDING.value,
        )

        # Update inbound file status
        await update_inbound_status(
            file_id=file_id,
            status=FileStatus.HITL.value,
            error_message=f"Escalated to HITL: {reason.value}",
        )

        print(f"[HITL] File {file_id} escalated: {reason.value}")
        return hitl_id


async def get_hitl_queue(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Get items from the HITL queue.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        if status:
            rows = await conn.fetch(
                """
                SELECT h.*, i.original_filename, i.file_extension,
                       i.file_path, i.source_type
                FROM hitl_queue h
                JOIN inbound_files_metadata i ON h.inbound_file_id = i.id
                WHERE h.status = $1
                ORDER BY h.created_at DESC
                LIMIT $2 OFFSET $3;
                """,
                status,
                limit,
                offset,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT h.*, i.original_filename, i.file_extension,
                       i.file_path, i.source_type
                FROM hitl_queue h
                JOIN inbound_files_metadata i ON h.inbound_file_id = i.id
                ORDER BY h.created_at DESC
                LIMIT $1 OFFSET $2;
                """,
                limit,
                offset,
            )
        return [dict(row) for row in rows]


async def get_hitl_item(hitl_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a specific HITL queue item.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT h.*, i.original_filename, i.file_extension,
                   i.file_path, i.source_type, i.sha256_hash
            FROM hitl_queue h
            JOIN inbound_files_metadata i ON h.inbound_file_id = i.id
            WHERE h.id = $1;
            """,
            hitl_id,
        )
        return dict(row) if row else None


async def approve_hitl_item(
    hitl_id: int,
    reviewer_id: str,
    final_classification: Optional[str] = None,
    final_routing: Optional[str] = None,
    reviewer_notes: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Approve a HITL queue item.

    If final_classification is not provided, uses original classification.
    """
    pool = await get_db_pool()

    # Get current item
    item = await get_hitl_item(hitl_id)
    if not item:
        return {"success": False, "error": "HITL item not found"}

    if item["status"] != HITLStatus.PENDING.value:
        return {"success": False, "error": f"Item already {item['status']}"}

    # Determine final values
    classification = final_classification or item.get("original_classification")
    routing = final_routing

    # Auto-determine routing if not specified
    if not routing and classification:
        if classification == Classification.UNSTRUCTURED.value:
            routing = RoutingDecision.VECTOR_PIPELINE.value
        elif classification == Classification.STRUCTURED.value:
            routing = RoutingDecision.SQL_OTL.value

    async with pool.acquire() as conn:
        # Update HITL record
        await conn.execute(
            """
            UPDATE hitl_queue
            SET status = $1, reviewer_id = $2, reviewer_notes = $3,
                final_classification = $4, final_routing = $5,
                reviewed_at = now()
            WHERE id = $6;
            """,
            HITLStatus.APPROVED.value,
            reviewer_id,
            reviewer_notes,
            classification,
            routing,
            hitl_id,
        )

        # Update inbound file - move back to classified status
        await update_inbound_status(
            file_id=str(item["inbound_file_id"]),
            status=FileStatus.CLASSIFIED.value,
            classification=classification,
            routed_to=routing,
            error_message=None,  # Clear error
        )

    return {
        "success": True,
        "hitl_id": hitl_id,
        "file_id": str(item["inbound_file_id"]),
        "final_classification": classification,
        "final_routing": routing,
        "message": "Item approved and ready for processing",
    }


async def reject_hitl_item(
    hitl_id: int,
    reviewer_id: str,
    reviewer_notes: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Reject a HITL queue item.

    The file will be marked as failed/rejected.
    """
    pool = await get_db_pool()

    item = await get_hitl_item(hitl_id)
    if not item:
        return {"success": False, "error": "HITL item not found"}

    if item["status"] != HITLStatus.PENDING.value:
        return {"success": False, "error": f"Item already {item['status']}"}

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE hitl_queue
            SET status = $1, reviewer_id = $2, reviewer_notes = $3,
                reviewed_at = now()
            WHERE id = $4;
            """,
            HITLStatus.REJECTED.value,
            reviewer_id,
            reviewer_notes,
            hitl_id,
        )

        # Update inbound file status to failed
        await update_inbound_status(
            file_id=str(item["inbound_file_id"]),
            status=FileStatus.FAILED.value,
            error_message=f"Rejected by reviewer: {reviewer_notes or 'No reason'}",
        )

    return {
        "success": True,
        "hitl_id": hitl_id,
        "file_id": str(item["inbound_file_id"]),
        "message": "Item rejected",
    }


async def get_hitl_stats() -> Dict[str, Any]:
    """
    Get HITL queue statistics.
    """
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'approved') as approved,
                COUNT(*) FILTER (WHERE status = 'rejected') as rejected,
                COUNT(*) as total
            FROM hitl_queue;
            """
        )
        return dict(stats) if stats else {}
