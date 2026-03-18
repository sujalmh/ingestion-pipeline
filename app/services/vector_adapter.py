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
# Upload to vector API
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
# Process (OCR + analysis)
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
# Vector ingestion
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
# Table extraction
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
# Worker
# ============================================================

async def _process_single_file_through_vector(
    file_path: str,
    source_url: Optional[str] = None,
    original_filename: Optional[str] = None,
) -> Dict[str, Any]:

    upload = await upload_to_vector_api(file_path)

    if not upload.get("success"):
        return {"success": False, "error": upload.get("error"), "failed_step": "upload"}

    process = await asyncio.to_thread(
        process_file_sync,
        file_path,
        upload["file_id"],
    )

    if not process.get("success"):
        return {"success": False, "error": process.get("error"), "failed_step": "analysis"}

    # ? Use original filename
    file_name = original_filename if original_filename else upload["file_name"]
    # override upload name explicitly
    upload["file_name"] = file_name

    ingest = await asyncio.to_thread(
        ingest_to_vector_sync,
        file_path,
        upload["file_path"],
        upload["file_id"],
        file_name,
        process["analysis"],
        source_url,
    )

    if not ingest.get("success"):
        return {"success": False, "error": ingest.get("error"), "failed_step": "ingest"}

    tables_result = None
    run_id = None

    if file_path.lower().endswith(".pdf"):

        try:
            tables = await asyncio.to_thread(
                extract_tables_sync,
                upload["file_path"],
            )

            tables_result = tables["result"]
            run_id = tables["run_id"]

        except Exception as e:
            logger.warning(f"Table extraction failed for {file_path}: {e}")

    return {
        "success": True,
        "vector_file_id": upload["file_id"],
        "file_name": file_name,
        "tables_result": tables_result,
        "run_id": run_id,
        "analysis": process.get("analysis"),
        "chunks_created": ingest.get("chunks_created"),
        "embeddings_generated": ingest.get("embeddings_generated"),
        "chunking_method": ingest.get("chunking_method"),
        "embedding_model": ingest.get("embedding_model"),
    }


# ============================================================
# Pipeline entry
# ============================================================

async def process_vector_pipeline(file_id: str) -> VectorPipelineResult:

    record = await get_inbound_file(file_id)
    logger.info(f"INBOUND DB RECORD ? {record}")
    if not record:
        return VectorPipelineResult(
            success=False,
            file_id=file_id,
            error="File not found",
            failed_step="init",
        )

    await update_inbound_status(file_id, "processing")

    file_path = record["file_path"]

    result = await _process_single_file_through_vector(
        file_path=file_path,
        source_url=record.get("source_url"),
        original_filename=record.get("original_filename") or record.get("file_name"),
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


# ============================================================
# Backward compatibility
# ============================================================

async def handoff_to_vector(file_id: str):
    return await process_vector_pipeline(file_id)

# ============================================================
# BULK PROCESSOR
# ============================================================

async def process_all_vector_files():

    from app.models.db import get_inbound_files

    files = await get_inbound_files(
        status="classified",
        source_type=None,
        limit=1000,
        offset=0,
    )

    vector_files = [
        f for f in files
        if f.get("routed_to") == "vector_pipeline"
    ]

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