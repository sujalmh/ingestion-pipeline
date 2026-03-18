"""
Pipeline Feeder — Feed converted files into the existing ingestion pipeline.

Takes files produced by the converter, registers them via the registration
service, triggers classification, and routes to the appropriate pipeline.

Pattern: Module-level async functions (adapter-style, like sql_adapter.py)
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles

from app.agent.source_config import SourceConfig, APIEndpointConfig
from app.models.schemas import SourceType

logger = logging.getLogger(__name__)


# ============ Public Functions ============

async def feed_file(
    file_path: str,
    filename: str,
    source_config: SourceConfig,
    api_config: APIEndpointConfig,
    metadata: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Feed a single file into the existing pipeline.

    Reads the file content, registers it via register_uploaded_file(),
    classifies it, and returns the result.

    Args:
        file_path: Absolute path to the converted file.
        filename: Original filename to register with.
        source_config: Source configuration.
        api_config: API endpoint configuration.
        metadata: Optional metadata hints for classification.

    Returns:
        Dict with registration and classification results.
    """
    from app.services.registration import register_uploaded_file
    from app.services.classification_orchestrator import classify_file_by_id

    try:
        # Step 1: Read file content
        fpath = Path(file_path)
        if not fpath.exists():
            return {
                "success": False,
                "error": f"File not found: {file_path}",
                "filename": filename,
            }

        async with aiofiles.open(fpath, "rb") as f:
            content = await f.read()

        if not content:
            return {
                "success": False,
                "error": "File is empty",
                "filename": filename,
            }

        # Step 2: Register the file
        source_url = source_config.base_url.rstrip("/") + api_config.endpoint
        upload_result = await register_uploaded_file(
            content=content,
            original_filename=filename,
            source_type=SourceType.DATA_API,
            source_identifier=f"{source_config.source_id}/{api_config.id}",
            source_url=source_url,
        )

        if not upload_result.success:
            if upload_result.is_duplicate:
                logger.info(
                    f"Skipping duplicate: {filename} "
                    f"(existing: {upload_result.existing_file_id})"
                )
                return {
                    "success": True,
                    "skipped": True,
                    "reason": "duplicate",
                    "filename": filename,
                    "existing_file_id": upload_result.existing_file_id,
                }
            return {
                "success": False,
                "error": upload_result.message,
                "filename": filename,
            }

        file_id = upload_result.file_id
        logger.info(f"Registered {filename} as file_id={file_id}")

        # Step 3: Classify the file
        # Use a stable table name derived from the source/API identifiers
        # (not the filename, which includes date and period labels).
        stable_table_name = f"{source_config.source_id}_{api_config.id}"
        try:
            cls_result = await classify_file_by_id(
                file_id, table_name=stable_table_name,
            )
            logger.info(
                f"Classified {filename}: "
                f"classification={cls_result.get('classification')}, "
                f"routed_to={cls_result.get('routed_to')}"
            )
        except Exception as e:
            logger.warning(f"Classification failed for {filename}: {e}")
            cls_result = {"success": False, "error": str(e)}

        # Step 4: Trigger processing based on routing decision
        # The agent must drive files all the way into the loader;
        # the UI flow waits for a human to call /process-sql, but
        # the agent pipeline needs to be end-to-end.
        sql_result = None
        routed_to = cls_result.get("routed_to", "")
        if cls_result.get("success") and routed_to.startswith("sql_"):
            sql_result = await _trigger_sql_processing(
                file_id, filename, routed_to,
                stable_table_name=stable_table_name,
                source_name=source_config.name,
                source_url=source_config.base_url,
            )

        result = {
            "success": True,
            "skipped": False,
            "filename": filename,
            "file_id": file_id,
            "classification": cls_result.get("classification"),
            "routed_to": routed_to,
            "classification_error": (
                cls_result.get("error")
                if not cls_result.get("success") else None
            ),
        }
        if sql_result:
            result["sql_job_id"] = sql_result.get("sql_job_id")
            result["sql_status"] = sql_result.get("sql_status")
            result["sql_error"] = sql_result.get("error")
            result["rows_inserted"] = sql_result.get("rows_inserted")
        return result

    except Exception as e:
        logger.error(f"Pipeline feeding failed for {filename}: {e}")
        return {
            "success": False,
            "error": str(e),
            "filename": filename,
        }


async def _trigger_sql_processing(
    file_id: str,
    filename: str,
    routed_to: str,
    stable_table_name: Optional[str] = None,
    source_name: Optional[str] = None,
    source_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Push a classified file all the way through the SQL loader.

    1. Upload to SQL Ingestion API (process_sql_pipeline)
    2. If status is awaiting_approval → auto-approve with metadata
    3. Wait for insertion to complete
    4. Update inbound_files_metadata status so OTL/INC routing works next time

    This makes the agent fully end-to-end with no manual steps.
    """
    import asyncio
    from app.models.db import update_inbound_status
    from app.services.sql_adapter import (
        process_sql_pipeline, approve_sql_job, get_sql_job_status,
    )

    sql_mode = "otl" if routed_to == "sql_otl" else "inc"
    table_name = stable_table_name or Path(filename).stem

    logger.info(
        f"Triggering SQL processing for {filename} "
        f"(file_id={file_id}, mode={sql_mode}, table={table_name})"
    )

    try:
        result = await process_sql_pipeline(
            file_id=file_id,
            preferred_table_name=table_name,
            skip_llm_table_name=True,
            preferred_sql_mode=sql_mode,
        )
        if not result.success:
            logger.warning(
                f"SQL upload failed for {filename}: {result.error}"
            )
            return {
                "success": False,
                "sql_job_id": result.sql_job_id,
                "sql_status": result.sql_status,
                "error": result.error,
            }

        sql_job_id = result.sql_job_id
        sql_status = result.sql_status
        logger.info(
            f"SQL upload done for {filename}: "
            f"job_id={sql_job_id}, status={sql_status}"
        )

        # Auto-approve if the SQL pipeline is waiting for approval
        if sql_status == "awaiting_approval" and sql_job_id:
            logger.info(f"Auto-approving SQL job {sql_job_id}")
            approve_result = await approve_sql_job(
                job_id=sql_job_id,
                table_name=table_name,
                source=source_name or "",
                source_url=source_url or "",
            )
            if not approve_result.get("success"):
                logger.warning(
                    f"Auto-approve failed for {sql_job_id}: "
                    f"{approve_result.get('error')}"
                )
                return {
                    "success": True,
                    "sql_job_id": sql_job_id,
                    "sql_status": "awaiting_approval",
                    "error": f"Uploaded but approval failed: {approve_result.get('error')}",
                }

            # Poll for completion (max ~5 minutes)
            final_status = await _wait_for_sql_completion(
                sql_job_id, table_name,
            )

            # Update inbound record so exists_completed_sql_load_for_table()
            # sees this load and routes subsequent runs to sql_inc.
            fs = final_status.get("status", "")
            if fs in ("completed", "incremental_load_completed"):
                await update_inbound_status(file_id, "done")
            elif fs in ("failed", "rejected"):
                await update_inbound_status(
                    file_id, "failed",
                    error_message=final_status.get("error"),
                )

            return {
                "success": final_status.get("success", True),
                "sql_job_id": sql_job_id,
                "sql_status": final_status.get("status"),
                "rows_inserted": final_status.get("rows_inserted"),
                "error": final_status.get("error"),
            }

        return {
            "success": True,
            "sql_job_id": sql_job_id,
            "sql_status": sql_status,
            "error": None,
        }

    except Exception as e:
        logger.error(f"SQL processing error for {filename}: {e}")
        return {"success": False, "error": str(e)}


async def _wait_for_sql_completion(
    sql_job_id: str,
    table_name: str,
    max_wait: int = 300,
    poll_interval: int = 5,
) -> Dict[str, Any]:
    """
    Poll the SQL pipeline for job completion after approval.
    Returns final status dict.
    """
    import asyncio
    from app.services.sql_adapter import get_sql_job_status

    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        try:
            status = await get_sql_job_status(sql_job_id)
        except Exception as e:
            logger.warning(f"Error polling SQL job {sql_job_id}: {e}")
            continue

        sql_status = status.get("status", "")

        if sql_status in ("completed", "incremental_load_completed"):
            result = status.get("result") or {}
            rows = result.get("rows_inserted", 0)
            logger.info(
                f"SQL job {sql_job_id} completed: "
                f"table={table_name}, rows_inserted={rows}"
            )
            return {
                "success": True,
                "status": sql_status,
                "rows_inserted": rows,
            }

        if sql_status in ("failed", "rejected"):
            logger.warning(
                f"SQL job {sql_job_id} {sql_status}: "
                f"{status.get('error')}"
            )
            return {
                "success": False,
                "status": sql_status,
                "error": status.get("error") or f"SQL job {sql_status}",
            }

        logger.info(
            f"SQL job {sql_job_id} still {sql_status} "
            f"({elapsed}s/{max_wait}s)"
        )

    logger.warning(
        f"SQL job {sql_job_id} did not complete within {max_wait}s"
    )
    return {
        "success": True,
        "status": "processing",
        "error": f"Job still processing after {max_wait}s — will complete in background",
    }


async def feed_all(
    converted_files: List[Dict[str, Any]],
    source_config: SourceConfig,
) -> Dict[str, Any]:
    """
    Feed multiple converted files into the pipeline.

    Args:
        converted_files: List of dicts with file_path, filename, api_config, metadata.
        source_config: Source configuration.

    Returns:
        Summary dict with counts and per-file results.
    """
    results = []
    ingested = 0
    skipped = 0
    failed = 0

    for file_info in converted_files:
        result = await feed_file(
            file_path=file_info["file_path"],
            filename=file_info["filename"],
            source_config=source_config,
            api_config=file_info["api_config"],
            metadata=file_info.get("metadata"),
        )
        results.append(result)

        if result.get("success"):
            if result.get("skipped"):
                skipped += 1
            else:
                ingested += 1
        else:
            failed += 1

    return {
        "total": len(results),
        "ingested": ingested,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }
