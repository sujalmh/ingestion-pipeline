"""
SQL Pipeline Adapter

Integrates with SQL Ingestion API:
- Upload structured files (CSV, Excel)
- Check job status
- Pass-through for approval/rejection (HITL handled by SQL Ingestion API)

API Endpoints:
- POST /upload - Upload file, returns job_id
- GET /status/{job_id} - Check status, get preview
- POST /approve/{job_id} - Approve with metadata
- POST /reject/{job_id} - Reject job

Workflow:
Upload → Preprocessing → Awaiting Approval → [Human Approves via SQL Ingestion API] → Insert → Completed
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass
import httpx

import logging

from app.config import settings
from app.models.db import (
    get_inbound_file,
    update_inbound_status,
    update_operational_metadata,
)

logger = logging.getLogger(__name__)


@dataclass
class SQLPipelineResult:
    """Result of SQL pipeline upload."""
    success: bool
    file_id: str
    
    # Job info from SQL Ingestion API
    sql_job_id: Optional[str] = None
    sql_status: Optional[str] = None
    
    # Preview (when awaiting_approval)
    preview: Optional[Dict[str, Any]] = None
    
    # Result (when completed)
    result: Optional[Dict[str, Any]] = None
    
    # Error info
    error: Optional[str] = None


async def upload_to_sql_pipeline(
    file_path: str,
    table_name: Optional[str] = None,
    skip_llm_table_name: bool = False,
    sql_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Upload a file to SQL Ingestion API.
    
    Args:
        file_path: Path to the file on disk (CSV or Excel)
        
    Returns:
        Dict with job_id, status, message
    """
    url = f"{settings.SQL_PIPELINE_API_URL}/upload"
    
    form_data: Dict[str, str] = {}
    if table_name:
        form_data["table_name"] = table_name
        if skip_llm_table_name:
            form_data["skip_llm_table_name"] = "true"
    if sql_mode in ("otl", "inc"):
        form_data["sql_mode"] = sql_mode

    async with httpx.AsyncClient(timeout=settings.SQL_UPLOAD_TIMEOUT) as client:
        with open(file_path, "rb") as f:
            files = {"file": f}
            response = await client.post(url, files=files, data=form_data or None)
            response.raise_for_status()
    
    data = response.json()
    
    return {
        "success": True,
        "job_id": data.get("job_id"),
        "status": data.get("status"),
        "message": data.get("message"),
    }


async def get_sql_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get status of a SQL pipeline job.
    
    Args:
        job_id: Job ID from SQL Ingestion API
        
    Returns:
        Dict with status, preview (if awaiting), result (if completed)
    """
    url = f"{settings.SQL_PIPELINE_API_URL}/status/{job_id}"
    
    async with httpx.AsyncClient(timeout=settings.SQL_STATUS_TIMEOUT) as client:
        response = await client.get(url)
        response.raise_for_status()
    
    data = response.json()
    
    return {
        "success": True,
        "job_id": data.get("job_id"),
        "status": data.get("status"),
        "preview": data.get("preview"),
        "result": data.get("result"),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
    }


async def process_sql_pipeline(
    file_id: str,
    preferred_table_name: Optional[str] = None,
    skip_llm_table_name: bool = False,
    preferred_sql_mode: Optional[str] = None,
) -> SQLPipelineResult:
    """
    Upload a file to SQL Ingestion API for processing.
    
    This will:
    1. Upload the file to SQL Ingestion API
    2. Get initial status
    3. Update our inbound_files_metadata with the SQL job_id
    
    The actual approval/rejection is handled by SQL Ingestion API directly.
    Users can check status and approve via SQL Ingestion API endpoints.
    
    Args:
        file_id: Our inbound_files_metadata file ID
        
    Returns:
        SQLPipelineResult with job info
    """
    # Get file info from our database
    file_record = await get_inbound_file(file_id)
    if not file_record:
        return SQLPipelineResult(
            success=False,
            file_id=file_id,
            error=f"File not found: {file_id}",
        )
    
    # Verify file is routed to SQL Ingestion API
    routed_to = file_record.get("routed_to")
    if not routed_to or not routed_to.startswith("sql_"):
        return SQLPipelineResult(
            success=False,
            file_id=file_id,
            error=f"File is not routed to SQL pipeline. Route: {routed_to}",
        )
    
    file_path = file_record["file_path"]
    
    # Update status to processing
    await update_inbound_status(file_id, "processing")
    
    try:
        # Upload to SQL Ingestion API
        try:
            upload_result = await upload_to_sql_pipeline(
                file_path,
                table_name=preferred_table_name,
                skip_llm_table_name=skip_llm_table_name,
                sql_mode=preferred_sql_mode,
            )
        except httpx.TimeoutException as e:
            await update_inbound_status(
                file_id, "failed",
                error_message=f"SQL Ingestion Pipeline timeout (upload): {str(e)}",
            )
            logger.warning("SQL Ingestion Pipeline upload timeout for file_id=%s: %s", file_id, e)
            return SQLPipelineResult(
                success=False,
                file_id=file_id,
                error=f"Timeout while uploading file to SQL Ingestion Pipeline ({settings.SQL_UPLOAD_TIMEOUT}s). {e}",
            )

        if not upload_result.get("success"):
            await update_inbound_status(
                file_id, "failed",
                error_message=f"SQL upload failed: {upload_result.get('error')}"
            )
            return SQLPipelineResult(
                success=False,
                file_id=file_id,
                error=upload_result.get("error"),
            )
        
        sql_job_id = upload_result["job_id"]

        # Get initial status with preview
        try:
            status_result = await get_sql_job_status(sql_job_id)
        except httpx.TimeoutException as e:
            await update_inbound_status(
                file_id, "failed",
                error_message=f"SQL Ingestion Pipeline timeout (status): {str(e)}",
            )
            logger.warning("SQL Ingestion Pipeline status timeout for file_id=%s job_id=%s: %s", file_id, sql_job_id, e)
            return SQLPipelineResult(
                success=False,
                file_id=file_id,
                sql_job_id=sql_job_id,
                error=f"Timeout while checking SQL Ingestion Pipeline job status ({settings.SQL_STATUS_TIMEOUT}s). {e}",
            )

        # Update our record - status stays "processing" until SQL Ingestion Pipeline completes
        # We store the SQL job_id for tracking
        # Note: We're using error_message field to store sql_job_id temporarily
        await update_inbound_status(
            file_id=file_id,
            status="processing",
            error_message=f"sql_job_id:{sql_job_id}",
        )
        
        # Update operational_metadata with SQL preview info
        op_meta_id = file_record.get("operational_metadata_id")
        preview = status_result.get("preview")
        if op_meta_id and preview:
            try:
                cols = preview.get("columns") or preview.get("column_names")
                if isinstance(cols, list) and cols:
                    # SQL Ingestion API may return list of dicts {name, type, sample_values} or list of strings
                    parts = []
                    for c in cols:
                        if isinstance(c, str):
                            parts.append(c)
                        elif isinstance(c, dict):
                            parts.append(c.get("name") or c.get("column_name") or str(c))
                        else:
                            parts.append(str(c))
                    col_str = ", ".join(parts)
                elif isinstance(cols, list):
                    col_str = None
                else:
                    col_str = str(cols) if cols else None
                row_count = preview.get("row_count") or preview.get("rows_count") or preview.get("total_rows")
                await update_operational_metadata(
                    record_id=op_meta_id,
                    rows_count=row_count,
                    columns=col_str,
                )
            except Exception as e:
                _cols = preview.get("columns") if isinstance(preview, dict) else None
                if _cols is None and isinstance(preview, dict):
                    _cols = preview.get("column_names")
                logger.warning(
                    "Failed to update operational_metadata for %s: %s (op_meta_id=%s, preview_keys=%s, columns_type=%s, columns_repr=%s)",
                    file_id,
                    e,
                    op_meta_id,
                    list(preview.keys()) if isinstance(preview, dict) else type(preview).__name__,
                    type(_cols).__name__ if _cols is not None else "None",
                    repr(_cols)[:500] if _cols is not None else "None",
                )

        return SQLPipelineResult(
            success=True,
            file_id=file_id,
            sql_job_id=sql_job_id,
            sql_status=status_result.get("status"),
            preview=status_result.get("preview"),
        )
        
    except httpx.TimeoutException as e:
        await update_inbound_status(
            file_id, "failed",
            error_message=f"SQL Ingestion Pipeline timeout: {str(e)}",
        )
        logger.warning("SQL pipeline timeout for file_id=%s: %s", file_id, e)
        return SQLPipelineResult(
            success=False,
            file_id=file_id,
            error=f"Timeout: {str(e)}",
        )
    except httpx.HTTPStatusError as e:
        await update_inbound_status(
            file_id, "failed",
            error_message=f"SQL API error: {e.response.status_code}"
        )
        return SQLPipelineResult(
            success=False,
            file_id=file_id,
            error=f"HTTP {e.response.status_code}: {e.response.text}",
        )
    except Exception as e:
        await update_inbound_status(
            file_id, "failed",
            error_message=f"SQL pipeline error: {str(e)}"
        )
        return SQLPipelineResult(
            success=False,
            file_id=file_id,
            error=str(e),
        )


async def check_sql_job_completion(file_id: str) -> Dict[str, Any]:
    """
    Check if a SQL job has completed and update our status accordingly.
    
    This should be called periodically or on-demand to sync status
    between SQL Ingestion API and our inbound_files_metadata.
    
    Args:
        file_id: Our inbound_files_metadata file ID
        
    Returns:
        Dict with current status
    """
    file_record = await get_inbound_file(file_id)
    if not file_record:
        return {"success": False, "error": "File not found"}
    
    # Extract sql_job_id from error_message (temporary storage)
    error_msg = file_record.get("error_message", "")
    if not error_msg.startswith("sql_job_id:"):
        return {"success": False, "error": "No SQL job ID found for this file"}
    
    sql_job_id = error_msg.replace("sql_job_id:", "")
    
    try:
        status_result = await get_sql_job_status(sql_job_id)
        sql_status = status_result.get("status")
        
        # Update our status based on SQL Ingestion Pipeline status (SQL Ingestion Pipeline may use "completed" or "incremental_load_completed")
        if sql_status in ("completed", "incremental_load_completed"):
            result = status_result.get("result", {})
            await update_inbound_status(
                file_id=file_id,
                status="done",
                error_message=None,  # Clear the job_id storage
            )
            return {
                "success": True,
                "file_id": file_id,
                "sql_job_id": sql_job_id,
                "status": "done",
                "result": result,
            }
        elif sql_status == "failed" or sql_status == "rejected":
            await update_inbound_status(
                file_id=file_id,
                status="failed",
                error_message=f"SQL job {sql_status}",
            )
            return {
                "success": True,
                "file_id": file_id,
                "sql_job_id": sql_job_id,
                "status": "failed",
                "sql_status": sql_status,
            }
        else:
            # Still processing or awaiting approval
            return {
                "success": True,
                "file_id": file_id,
                "sql_job_id": sql_job_id,
                "status": "processing",
                "sql_status": sql_status,
                "preview": status_result.get("preview"),
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


async def handoff_to_sql(file_id: str) -> Dict[str, Any]:
    """
    Handoff function for router - triggers SQL pipeline processing.
    
    This is called by the router when a file is routed to sql_otl or sql_inc.
    
    Args:
        file_id: Our inbound_files_metadata file ID
        
    Returns:
        Dict with handoff status (processing happens async)
    """
    # For now, just return acknowledgment - actual processing via process-sql endpoint
    # In production, this could trigger async processing
    return {
        "status": "ready_for_processing",
        "file_id": file_id,
        "message": "File ready for SQL processing. Call /process-sql endpoint.",
    }


async def process_all_sql_files() -> Dict[str, Any]:
    """
    Process all files that are classified and routed to sql_* pipelines.
    Uses asyncio.gather with a semaphore for parallel processing.
    
    Returns:
        Summary of processing results
    """
    import asyncio
    from app.models.db import get_inbound_files
    
    # Get all files routed to SQL pipelines with status 'classified'
    files_otl = await get_inbound_files(
        status="classified",
        routed_to="sql_otl",
        limit=100,
    )
    files_inc = await get_inbound_files(
        status="classified",
        routed_to="sql_inc",
        limit=100,
    )
    
    files = files_otl + files_inc

    if not files:
        return {"total": 0, "successful": 0, "failed": 0, "processed": []}

    sem = asyncio.Semaphore(settings.PARALLEL_FILE_LIMIT)

    async def _process_one(file_record):
        async with sem:
            file_id = file_record["id"]
            result = await process_sql_pipeline(file_id)
            return {
                "file_id": file_id,
                "filename": file_record["original_filename"],
                "success": result.success,
                "sql_job_id": result.sql_job_id,
                "sql_status": result.sql_status,
                "error": result.error,
            }

    processed = await asyncio.gather(*[_process_one(f) for f in files])

    successful = sum(1 for p in processed if p["success"])
    failed = sum(1 for p in processed if not p["success"])

    return {
        "total": len(processed),
        "successful": successful,
        "failed": failed,
        "processed": list(processed),
    }
