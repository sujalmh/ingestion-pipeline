# -*- coding: utf-8 -*-
"""
Vector Pipeline Adapter (NON-STREAMING)

Behaviour:
- Blocking execution (from caller POV)
- Async-safe for FastAPI
- Vector ingestion via 8071
- Table extraction via 8067
- Returns unified result for inbound (8073)
"""

import json
import logging
import asyncio
import time
import uuid
from typing import Dict, Any, Optional
from dataclasses import dataclass

import httpx
import requests

from app.config import settings
from app.models.db import get_inbound_file, update_inbound_status, update_operational_metadata

logger = logging.getLogger(__name__)

# ============================================================
# Data model
# ============================================================
@dataclass
class VectorPipelineResult:
    success: bool
    file_id: str
    vector_file_id: Optional[str] = None
    chunks_created: Optional[int] = None
    embeddings_generated: Optional[int] = None
    collection_name: Optional[str] = None
    tables_extracted: Optional[Dict[str, Any]] = None
    run_id: Optional[str] = None
    error: Optional[str] = None
    failed_step: Optional[str] = None


# ============================================================
# Upload to vector API (async-safe)
# ============================================================
async def upload_to_vector_api(file_path: str) -> Dict[str, Any]:
    url = f"{settings.VECTOR_INGEST_API_URL}/upload-files/"
    async with httpx.AsyncClient(timeout=settings.VECTOR_UPLOAD_TIMEOUT) as client:
        with open(file_path, "rb") as f:
            resp = await client.post(url, files={"files": f})
            resp.raise_for_status()

    data = resp.json()

    if data.get("new_files"):
        f = data["new_files"][0]
        return {
            "success": True,
            "file_id": f["id"],
            "file_name": f["name"],
            "file_path": f["path"],
            "is_duplicate": False,
        }

    if data.get("duplicates"):
        f = data["duplicates"][0]
        dup_id = f["existing_file_id"]
        dup_name = f.get("name", "")
        import os
        ext = os.path.splitext(dup_name)[1] if dup_name else os.path.splitext(file_path)[1]
        vm_path = f"/data/vl/uploaded_files/{dup_id}{ext}"
        return {
            "success": True,
            "file_id": dup_id,
            "file_name": dup_name,
            "file_path": vm_path,
            "is_duplicate": True,
        }

    return {"success": False, "error": "Upload failed"}


# ============================================================
# Process (OCR + analysis) ? SSE consumer
# ============================================================
def process_file_sync(file_path: str, file_id: str) -> Dict[str, Any]:
    url = f"{settings.VECTOR_INGEST_API_URL}/process-files"

    timeout = httpx.Timeout(
        connect=30.0,
        read=settings.VECTOR_PROCESS_TIMEOUT,
        write=120.0,
        pool=30.0,
    )

    with httpx.Client(timeout=timeout) as client:
        with open(file_path, "rb") as f:
            with client.stream(
                "POST",
                url,
                files={"files": f},
                data={"file_ids": file_id},
            ) as resp:
                resp.raise_for_status()

                last_event = None
                for line in resp.iter_lines():
                    if not line or not line.startswith("data:"):
                        continue

                    try:
                        event = json.loads(line.replace("data:", "").strip())
                    except json.JSONDecodeError:
                        continue

                    if event.get("status") == "error":
                        return {
                            "success": False,
                            "error": event.get("message", "Analysis failed"),
                        }

                    last_event = event

                if last_event and "analysis" in last_event:
                    return {
                        "success": True,
                        "analysis": last_event["analysis"],
                    }

    return {"success": False, "error": "No analysis returned"}


# ============================================================
# Vector ingestion (blocking)
# ============================================================
def ingest_to_vector_sync(
    local_file_path: str,
    vector_file_path: str,
    file_id: str,
    file_name: str,
    analysis: Dict[str, Any],
    source_url: Optional[str],
) -> Dict[str, Any]:

    url = f"{settings.VECTOR_INGEST_API_URL}/ingest/"

    file_details = [{
        "id": file_id,
        "name": file_name,
        "path": vector_file_path,
        "analysis": analysis,
        "sourceUrl": source_url or "vector-adapter",
    }]

    with open(local_file_path, "rb") as f:
        resp = requests.post(
            url,
            files={"files": f},
            data={
                "file_details": json.dumps(file_details),
                "collection_name": settings.MILVUS_COLLECTION_NAME,
            },
            timeout=settings.VECTOR_INGEST_TIMEOUT,
        )

    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        return {"success": False, "error": "Vector ingest failed"}

    # Preserve ingestion details returned by the vector API
    ingestion_details = data.get("ingestionDetails") or {}
    return {
        "success": True,
        "chunks_created": ingestion_details.get("chunksCreated"),
        "embeddings_generated": ingestion_details.get("embeddingsGenerated"),
        "chunking_method": ingestion_details.get("chunkingMethod"),
        "embedding_model": ingestion_details.get("embeddingModel"),
        "collection": ingestion_details.get("collection"),
    }


# ============================================================
# Table extraction via 8067 (blocking)
# ============================================================
def extract_tables_sync(file_path: str) -> Dict[str, Any]:
    run_id = f"run_{time.strftime('%Y_%m_%d')}_{uuid.uuid4().hex[:6]}"

    resp = requests.post(
        f"{settings.VECTOR_EXTRACT_API_URL}/extract/tables",
        params={
            "file_path": file_path,
            "run_id": run_id,
        },
        timeout=settings.VECTOR_EXTRACT_TIMEOUT,
    )

    resp.raise_for_status()

    return {
        "run_id": run_id,
        "result": resp.json(),
    }


# ============================================================
# PIPELINE ORCHESTRATOR (async-safe)
# ============================================================
async def _process_single_file_through_vector(
    file_path: str,
    source_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Process a single file (or batch-split) through the full vector pipeline
    steps: upload -> OCR/analysis -> ingest -> table-extract (PDFs only).

    Returns a dict with all result fields, or an error dict.
    """
    # ---- Upload
    upload = await upload_to_vector_api(file_path)
    if not upload.get("success"):
        return {"success": False, "error": upload.get("error"), "failed_step": "upload"}

    # ---- OCR + analysis
    process = await asyncio.to_thread(
        process_file_sync,
        file_path,
        upload["file_id"],
    )
    if not process.get("success"):
        return {"success": False, "error": process.get("error"), "failed_step": "analysis"}

    # ---- Vector ingestion
    ingest = await asyncio.to_thread(
        ingest_to_vector_sync,
        file_path,
        upload["file_path"],
        upload["file_id"],
        upload["file_name"],
        process["analysis"],
        source_url,
    )
    if not ingest.get("success"):
        return {"success": False, "error": ingest.get("error"), "failed_step": "ingest"}

    # ---- Table extraction (8067) -- PDFs only, uses VM path
    tables_result = None
    run_id = None
    if file_path.lower().endswith(".pdf"):
        try:
            tables = await asyncio.to_thread(
                extract_tables_sync,
                upload["file_path"],  # VM path, not local path
            )
            tables_result = tables["result"]
            run_id = tables["run_id"]
        except Exception as e:
            logger.warning(f"Table extraction failed for {file_path}: {e}")

    return {
        "success": True,
        "vector_file_id": upload["file_id"],
        "file_name": upload["file_name"],
        "tables_result": tables_result,
        "run_id": run_id,
        "analysis": process.get("analysis"),
        "chunks_created": ingest.get("chunks_created"),
        "embeddings_generated": ingest.get("embeddings_generated"),
        "chunking_method": ingest.get("chunking_method"),
        "embedding_model": ingest.get("embedding_model"),
    }


async def process_vector_pipeline(file_id: str) -> VectorPipelineResult:
    import os

    record = await get_inbound_file(file_id)
    if not record:
        return VectorPipelineResult(
            success=False,
            file_id=file_id,
            error="File not found",
            failed_step="init",
        )

    await update_inbound_status(file_id, "processing")

    file_path = record["file_path"]
    file_ext = record.get("file_extension", "").lower()
    file_size = record.get("file_size") or 0

    # ---------------------------------------------------------------
    # Check if we should split this PDF into batches for parallelism
    # ---------------------------------------------------------------
    is_large_pdf = (
        file_ext == ".pdf"
        and file_size > settings.pdf_batch_threshold_bytes
    )

    if is_large_pdf:
        return await _process_large_pdf(file_id, record)

    # ---------------------------------------------------------------
    # Standard single-file processing (unchanged behaviour)
    # ---------------------------------------------------------------
    result = await _process_single_file_through_vector(
        file_path=file_path,
        source_url=record.get("source_url"),
    )

    if not result["success"]:
        return VectorPipelineResult(
            success=False,
            file_id=file_id,
            error=result.get("error"),
            failed_step=result.get("failed_step"),
        )

    await update_inbound_status(
        file_id=file_id,
        status="done",
        vector_file_id=result["vector_file_id"],
    )

    _update_op_meta(record, result)

    return VectorPipelineResult(
        success=True,
        file_id=file_id,
        vector_file_id=result["vector_file_id"],
        chunks_created=result.get("chunks_created"),
        embeddings_generated=result.get("embeddings_generated"),
        collection_name=settings.MILVUS_COLLECTION_NAME,
        tables_extracted=result.get("tables_result"),
        run_id=result.get("run_id"),
    )


async def _process_large_pdf(
    file_id: str,
    record: Dict[str, Any],
) -> VectorPipelineResult:
    """
    Split a large PDF into page-range batches and process each batch in
    parallel through the vector pipeline.
    """
    import os
    from app.services.pdf_splitter import split_pdf

    file_path = record["file_path"]
    logger.info(
        "Large PDF detected (%s, %.1f MB). Splitting into batches of %d pages.",
        file_path,
        (record.get("file_size") or 0) / (1024 * 1024),
        settings.PDF_BATCH_PAGE_COUNT,
    )

    # Split on a thread (CPU-bound)
    batch_paths = await asyncio.to_thread(
        split_pdf, file_path, settings.PDF_BATCH_PAGE_COUNT,
    )

    sem = asyncio.Semaphore(settings.PARALLEL_FILE_LIMIT)

    async def _process_batch(bp: str) -> Dict[str, Any]:
        async with sem:
            return await _process_single_file_through_vector(
                file_path=bp,
                source_url=record.get("source_url"),
            )

    batch_results = await asyncio.gather(
        *[_process_batch(bp) for bp in batch_paths],
        return_exceptions=True,
    )

    # Clean up temp split files (skip the original)
    for bp in batch_paths:
        if bp != file_path:
            try:
                os.unlink(bp)
            except OSError:
                pass

    # Aggregate results
    all_vector_ids = []
    all_tables = []
    all_run_ids = []
    errors = []

    for i, br in enumerate(batch_results):
        if isinstance(br, Exception):
            errors.append(f"Batch {i}: {br}")
            continue
        if not br.get("success"):
            errors.append(f"Batch {i}: {br.get('error', 'unknown error')}")
            continue
        all_vector_ids.append(br["vector_file_id"])
        if br.get("tables_result"):
            all_tables.append(br["tables_result"])
        if br.get("run_id"):
            all_run_ids.append(br["run_id"])

    if not all_vector_ids:
        # Every batch failed
        return VectorPipelineResult(
            success=False,
            file_id=file_id,
            error=f"All {len(batch_results)} batches failed: {'; '.join(errors)}",
            failed_step="batch_processing",
        )

    primary_vector_id = all_vector_ids[0]

    await update_inbound_status(
        file_id=file_id,
        status="done",
        vector_file_id=primary_vector_id,
    )

    # Merge tables results
    merged_tables = None
    if all_tables:
        merged_tables = {
            "batches": len(all_tables),
            "batch_results": all_tables,
        }

    # Use the analysis from the first successful batch for metadata enrichment
    first_success = next(
        (br for br in batch_results
         if not isinstance(br, Exception) and br.get("success")),
        {},
    )

    _update_op_meta(record, {
        "vector_file_id": primary_vector_id,
        "tables_result": merged_tables,
        "analysis": first_success.get("analysis"),
        "chunks_created": first_success.get("chunks_created"),
        "embeddings_generated": first_success.get("embeddings_generated"),
        "chunking_method": first_success.get("chunking_method"),
        "embedding_model": first_success.get("embedding_model"),
    })

    logger.info(
        "Large PDF %s processed: %d/%d batches succeeded, vector_ids=%s",
        file_path, len(all_vector_ids), len(batch_results), all_vector_ids,
    )

    return VectorPipelineResult(
        success=True,
        file_id=file_id,
        vector_file_id=primary_vector_id,
        collection_name=settings.MILVUS_COLLECTION_NAME,
        tables_extracted=merged_tables,
        run_id=", ".join(all_run_ids) if all_run_ids else None,
        error="; ".join(errors) if errors else None,
    )


def _update_op_meta(record: Dict[str, Any], result: Dict[str, Any]):
    """
    Update operational_metadata with enriched data from Suyog's vector API.

    The analysis dict (from /process-files) contains:
        domain, subdomain, brief_summary, publishing_authority,
        published_date, period_of_reference, quality_score, etc.

    The ingest response (from /ingest/) contains:
        chunks_created, embeddings_generated, chunking_method, embedding_model.
    """
    import asyncio

    op_meta_id = record.get("operational_metadata_id")
    if not op_meta_id:
        return

    async def _do():
        try:
            analysis = result.get("analysis") or {}

            # --- Build the fields to update from the analysis ---
            major_domain = analysis.get("domain")
            sub_domain = analysis.get("subdomain")
            brief_summary = analysis.get("brief_summary")
            period_cols = analysis.get("period_of_reference")

            # --- Build columns string from ingestion details ---
            col_parts = []
            if result.get("chunks_created") is not None:
                col_parts.append(f"chunks: {result['chunks_created']}")
            if result.get("embeddings_generated") is not None:
                col_parts.append(f"embeddings: {result['embeddings_generated']}")
            if result.get("embedding_model"):
                col_parts.append(f"model: {result['embedding_model']}")
            col_parts.append(f"vector_file_id: {result.get('vector_file_id')}")
            col_parts.append(f"collection: {settings.MILVUS_COLLECTION_NAME}")
            columns = ", ".join(col_parts)

            # --- Build business_metadata with extra analysis fields ---
            biz_meta_parts = {}
            if analysis.get("publishing_authority") and analysis["publishing_authority"] != "unknown":
                biz_meta_parts["publishing_authority"] = analysis["publishing_authority"]
            if analysis.get("published_date") and analysis["published_date"] != "unknown":
                biz_meta_parts["published_date"] = analysis["published_date"]
            if analysis.get("quality_score") is not None:
                biz_meta_parts["quality_score"] = analysis["quality_score"]
            if analysis.get("intents"):
                biz_meta_parts["intents"] = analysis["intents"]
            if result.get("chunking_method"):
                biz_meta_parts["chunking_method"] = result["chunking_method"]
            business_metadata = json.dumps(biz_meta_parts) if biz_meta_parts else None

            # --- Table extraction count ---
            tr = result.get("tables_result")
            tables_count = None
            if isinstance(tr, dict) and tr.get("summary"):
                tables_count = tr["summary"].get("total_tables_extracted")
            elif isinstance(tr, list):
                tables_count = len(tr)
            elif tr:
                tables_count = 1

            await update_operational_metadata(
                record_id=op_meta_id,
                major_domain=major_domain,
                sub_domain=sub_domain,
                brief_summary=brief_summary,
                rows_count=tables_count,
                columns=columns,
                period_cols=period_cols,
                business_metadata=business_metadata,
            )
            logger.info(
                "Updated operational_metadata %s with vector analysis "
                "(domain=%s, subdomain=%s, chunks=%s)",
                op_meta_id, major_domain, sub_domain,
                result.get("chunks_created"),
            )
        except Exception as e:
            logger.warning(f"Failed to update operational_metadata: {e}")

    asyncio.ensure_future(_do())


# ============================================================
# BACKWARD COMPATIBILITY SHIMS
# ============================================================
async def handoff_to_vector(file_id: str):
    return await process_vector_pipeline(file_id)


async def process_all_vector_files():
    from app.models.db import get_inbound_files

    files = await get_inbound_files(
        status="classified",
        source_type=None,
        limit=1000,
        offset=0,
    )

    vector_files = [f for f in files if f.get("routed_to") == "vector_pipeline"]

    if not vector_files:
        return {"total": 0, "successful": 0, "failed": 0, "processed": []}

    sem = asyncio.Semaphore(settings.PARALLEL_FILE_LIMIT)

    async def _process_one(f):
        async with sem:
            result = await process_vector_pipeline(f["id"])
            return {
                "file_id": f["id"],
                "success": result.success,
                "error": result.error,
            }

    processed = await asyncio.gather(*[_process_one(f) for f in vector_files])

    successful = sum(1 for p in processed if p["success"])
    failed = sum(1 for p in processed if not p["success"])

    return {
        "total": len(processed),
        "successful": successful,
        "failed": failed,
        "processed": list(processed),
    }
