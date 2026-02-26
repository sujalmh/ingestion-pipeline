"""
Automated Ingestion Pipeline - FastAPI Application

This API handles:
1. File upload and registration
2. File classification (structured vs unstructured)
3. Routing to appropriate pipeline (vector or SQL)
4. Status tracking and updates
"""
import os
import logging
from pathlib import Path
from typing import List, Optional
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from app.config import settings
from app.models.db import (
    connect_db,
    disconnect_db,
    init_all_tables,
    get_inbound_file,
    get_inbound_files,
    update_inbound_retry_count,
    update_inbound_status,
)
from app.models.schemas import (
    SourceType,
    InboundFileResponse,
    InboundFileStatus,
    UploadResponse,
    BatchUploadResponse,
)
from app.services.registration import register_uploaded_file


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler for startup and shutdown.
    """
    # --- Startup ---
    print("🚀 Starting Automated Ingestion Pipeline...")
    
    # Create upload directory
    upload_dir = Path(settings.UPLOAD_DIRECTORY)
    upload_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Upload directory: {upload_dir.resolve()}")
    
    # Initialize database
    await connect_db()
    await init_all_tables()
    
    print("✅ Startup complete!")
    
    yield
    
    # --- Shutdown ---
    print("🛑 Shutting down...")
    await disconnect_db()
    print("✅ Shutdown complete.")


# Create FastAPI app
app = FastAPI(
    title="Automated Ingestion Pipeline",
    description="""
    API for automated file ingestion with classification and routing.
    
    ## Features
    - File upload with duplicate detection
    - Automatic classification (structured vs unstructured)
    - Routing to vector or SQL pipeline
    - Status tracking
    """,
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002",
    "http://100.104.12.231",
    "http://100.104.12.231:8073",
    "http://100.104.12.231:8002",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# Health Check
# ==========================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "automated-ingestion-pipeline",
        "version": "1.0.0"
    }


# ==========================================
# File Upload Endpoints
# ==========================================

@app.post("/inbound/upload", response_model=UploadResponse, tags=["Inbound Files"])
async def upload_file(
    file: UploadFile = File(...),
    source_type: SourceType = Form(default=SourceType.DIRECT_UPLOAD),
    source_identifier: Optional[str] = Form(default=None),
    source_url: Optional[str] = Form(default=None),
):
    """
    Upload a single file for processing.
    
    The file will be:
    1. Validated (size, extension)
    2. Hashed for duplicate detection
    3. Stored with a unique filename
    4. Registered in the inbound_files_metadata table
    
    Returns a tracking ID for status queries.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")
    
    # Read file content
    content = await file.read()
    
    # Register the file
    result = await register_uploaded_file(
        content=content,
        original_filename=file.filename,
        source_type=source_type,
        source_identifier=source_identifier,
        source_url=source_url,
    )
    
    if not result.success and not result.is_duplicate:
        raise HTTPException(status_code=400, detail=result.message)
    
    return result


@app.post("/inbound/upload/batch", response_model=BatchUploadResponse, tags=["Inbound Files"])
async def upload_files_batch(
    files: List[UploadFile] = File(...),
    source_type: SourceType = Form(default=SourceType.DIRECT_UPLOAD),
    source_identifier: Optional[str] = Form(default=None),
):
    """
    Upload multiple files for processing.
    
    Each file is processed independently. Returns summary of results.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided.")
    
    results: List[UploadResponse] = []
    successful = 0
    duplicates = 0
    failed = 0
    
    for file in files:
        if not file.filename:
            results.append(UploadResponse(
                success=False,
                message="No filename provided.",
                filename="unknown",
            ))
            failed += 1
            continue
        
        content = await file.read()
        
        result = await register_uploaded_file(
            content=content,
            original_filename=file.filename,
            source_type=source_type,
            source_identifier=source_identifier,
        )
        
        results.append(result)
        
        if result.success:
            successful += 1
        elif result.is_duplicate:
            duplicates += 1
        else:
            failed += 1
    
    return BatchUploadResponse(
        success=failed == 0,
        message=f"Processed {len(files)} file(s). Successful: {successful}, Duplicates: {duplicates}, Failed: {failed}",
        total_files=len(files),
        successful=successful,
        duplicates=duplicates,
        failed=failed,
        files=results,
    )


# ==========================================
# Status Endpoints
# ==========================================

@app.get("/inbound/files", tags=["Status"])
async def list_inbound_files(
    status: Optional[str] = Query(default=None, description="Filter by status"),
    source_type: Optional[str] = Query(default=None, description="Filter by source type"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """
    List all inbound files with optional filtering.
    """
    files = await get_inbound_files(
        status=status,
        source_type=source_type,
        limit=limit,
        offset=offset,
    )
    
    return {
        "success": True,
        "count": len(files),
        "files": files,
    }


@app.get("/inbound/files/{file_id}", tags=["Status"])
async def get_file_details(file_id: str):
    """
    Get detailed information about a specific file.
    """
    file_record = await get_inbound_file(file_id)
    
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File with ID '{file_id}' not found.")
    
    return {
        "success": True,
        "file": file_record,
    }


@app.get("/inbound/files/{file_id}/status", response_model=InboundFileStatus, tags=["Status"])
async def get_file_status(file_id: str):
    """
    Get quick status check for a file.
    
    Lightweight endpoint for polling file status.
    """
    file_record = await get_inbound_file(file_id)
    
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File with ID '{file_id}' not found.")
    
    return InboundFileStatus(
        id=file_record["id"],
        status=file_record["status"],
        updated_at=file_record.get("processing_completed_at") 
                   or file_record.get("processing_started_at")
                   or file_record.get("classification_completed_at")
                   or file_record["created_at"],
    )


@app.post("/inbound/files/{file_id}/retry", tags=["Status"])
async def retry_failed_file(file_id: str):
    """
    Retry processing a failed file.
    
    Resets the file status to 'open_request' and increments retry count.
    """
    file_record = await get_inbound_file(file_id)
    
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File with ID '{file_id}' not found.")
    
    if file_record["status"] != "failed":
        raise HTTPException(
            status_code=400, 
            detail=f"Can only retry failed files. Current status: {file_record['status']}"
        )
    
    new_retry_count = await update_inbound_retry_count(file_id)
    
    return {
        "success": True,
        "message": "File queued for retry.",
        "file_id": file_id,
        "retry_count": new_retry_count,
        "status": "open_request",
    }


# ==========================================
# Classification Endpoints
# ==========================================

@app.post("/inbound/files/{file_id}/classify", tags=["Classification"])
async def classify_single_file(file_id: str):
    """
    Classify a single file and extract metadata.

    This will:
    1. Determine if file is structured or unstructured
    2. Extract domain, subdomain, summary via LLM
    3. Create operational_metadata entry
    4. Update file status to 'classified'
    5. Set routing decision (vector_pipeline, sql_otl, hitl)
    """
    from app.services.classification_orchestrator import classify_file_by_id

    result = await classify_file_by_id(file_id)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))

    return result


@app.post("/inbound/classify/all", tags=["Classification"])
async def classify_all_pending_files():
    """
    Classify all files with status 'open_request'.

    Processes all pending files and returns summary.
    """
    from app.services.classification_orchestrator import classify_all_pending

    result = await classify_all_pending()
    return result


@app.post("/inbound/upload-and-classify", tags=["Classification"])
async def upload_and_classify(
    file: UploadFile = File(...),
    source_type: SourceType = Form(default=SourceType.DIRECT_UPLOAD),
    source_identifier: Optional[str] = Form(default=None),
    source_url: Optional[str] = Form(default=None),
):
    """
    Upload a file and immediately classify it.

    Combines upload and classification in one step.
    Returns full classification results including routing decision.
    """
    from app.services.classification_orchestrator import classify_file_by_id

    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")

    # Read file content
    content = await file.read()

    # Register the file
    upload_result = await register_uploaded_file(
        content=content,
        original_filename=file.filename,
        source_type=source_type,
        source_identifier=source_identifier,
        source_url=source_url,
    )

    if not upload_result.success:
        if upload_result.is_duplicate:
            return {
                "success": False,
                "message": "File is a duplicate",
                "is_duplicate": True,
                "existing_file_id": upload_result.existing_file_id,
            }
        raise HTTPException(status_code=400, detail=upload_result.message)

    file_id = upload_result.file_id
    if not file_id:
        raise HTTPException(status_code=500, detail="Upload succeeded but file_id is missing.")

    # Classify the file
    classification_result = await classify_file_by_id(file_id)

    return {
        "success": True,
        "upload": {
            "file_id": file_id,
            "filename": upload_result.filename,
        },
        "classification": classification_result,
    }


# ==========================================
# Routing Endpoints
# ==========================================

@app.post("/inbound/files/{file_id}/route", tags=["Routing"])
async def route_single_file(file_id: str):
    """
    Route a classified file to the appropriate pipeline.

    Routes based on the classification result:
    - unstructured -> vector_pipeline
    - structured -> sql_pipeline
    - low confidence -> hitl
    """
    from app.services.router import route_file

    result = await route_file(file_id)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))

    return result


@app.post("/inbound/route/all", tags=["Routing"])
async def route_all_classified_files():
    """
    Route all files with status 'classified'.
    """
    from app.services.router import route_all_classified

    result = await route_all_classified()
    return result


# ==========================================
# HITL Endpoints
# ==========================================

@app.get("/hitl/queue", tags=["HITL"])
async def get_hitl_queue_items(
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0),
):
    """
    Get items from the HITL queue.

    Filter by status: pending, approved, rejected
    """
    from app.services.hitl import get_hitl_queue

    items = await get_hitl_queue(status=status, limit=limit, offset=offset)
    return {
        "success": True,
        "count": len(items),
        "items": items,
    }


@app.get("/hitl/queue/{hitl_id}", tags=["HITL"])
async def get_hitl_queue_item(hitl_id: int):
    """
    Get a specific HITL queue item.
    """
    from app.services.hitl import get_hitl_item

    item = await get_hitl_item(hitl_id)
    if not item:
        raise HTTPException(status_code=404, detail="HITL item not found")

    return {"success": True, "item": item}


@app.post("/hitl/queue/{hitl_id}/approve", tags=["HITL"])
async def approve_hitl_queue_item(
    hitl_id: int,
    reviewer_id: str = Form(...),
    final_classification: Optional[str] = Form(default=None),
    final_routing: Optional[str] = Form(default=None),
    reviewer_notes: Optional[str] = Form(default=None),
):
    """
    Approve a HITL queue item.

    Optionally override classification and routing.
    """
    from app.services.hitl import approve_hitl_item

    result = await approve_hitl_item(
        hitl_id=hitl_id,
        reviewer_id=reviewer_id,
        final_classification=final_classification,
        final_routing=final_routing,
        reviewer_notes=reviewer_notes,
    )

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))

    return result


@app.post("/hitl/queue/{hitl_id}/reject", tags=["HITL"])
async def reject_hitl_queue_item(
    hitl_id: int,
    reviewer_id: str = Form(...),
    reviewer_notes: Optional[str] = Form(default=None),
):
    """
    Reject a HITL queue item.
    """
    from app.services.hitl import reject_hitl_item

    result = await reject_hitl_item(
        hitl_id=hitl_id,
        reviewer_id=reviewer_id,
        reviewer_notes=reviewer_notes,
    )

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))

    return result


@app.get("/hitl/stats", tags=["HITL"])
async def get_hitl_statistics():
    """
    Get HITL queue statistics.
    """
    from app.services.hitl import get_hitl_stats

    stats = await get_hitl_stats()
    return {"success": True, "stats": stats}


# ==========================================
# Vector Pipeline Endpoints
# ==========================================

@app.post("/inbound/files/{file_id}/process-vector", tags=["Vector Pipeline"])
async def process_file_vector(file_id: str):
    """
    Process a single file through the vector pipeline.
    
    The file must be:
    1. Already classified
    2. Routed to 'vector_pipeline'
    
    This will:
    1. Upload to vector API
    2. Process (OCR + analysis)
    3. Generate embeddings and store in Milvus
    4. Extract tables (for PDFs)
    """
    from app.services.vector_adapter import process_vector_pipeline
    
    result = await process_vector_pipeline(file_id)
    
    if not result.success:
        raise HTTPException(
            status_code=400,
            detail={
                "error": result.error,
                "failed_step": result.failed_step,
            }
        )
    
    return {
        "success": True,
        "file_id": result.file_id,
        "vector_file_id": result.vector_file_id,
        "chunks_created": result.chunks_created,
        "embeddings_generated": result.embeddings_generated,
        "collection_name": result.collection_name,
        "tables_extracted": result.tables_extracted,
        "run_id": result.run_id,
    }


@app.post("/inbound/process-vector/all", tags=["Vector Pipeline"])
async def process_all_vector():
    """
    Process all files that are classified and routed to vector_pipeline.
    
    Processes files in sequence and returns summary.
    """
    from app.services.vector_adapter import process_all_vector_files
    
    result = await process_all_vector_files()
    return {
        "success": True,
        "summary": {
            "total": result["total"],
            "successful": result["successful"],
            "failed": result["failed"],
        },
        "processed": result["processed"],
    }


@app.post("/inbound/upload-classify-process", tags=["Full Pipeline"])
async def upload_classify_and_process(
    file: UploadFile = File(...),
    source_type: SourceType = Form(default=SourceType.DIRECT_UPLOAD),
    source_identifier: Optional[str] = Form(default=None),
    source_url: Optional[str] = Form(default=None),
    sql_mode: Optional[str] = Form(default=None),
    table_name: Optional[str] = Form(default=None),
):
    """
    Complete end-to-end processing: Upload -> Classify -> Route to appropriate pipeline.
    
    - Unstructured files -> Vector pipeline (upload, process, ingest, extract tables)
    - Structured files -> SQL pipeline (upload to SQL pipeline API, returns job_id for approval)
    - Low confidence -> HITL queue for human review
    """
    from app.services.classification_orchestrator import classify_file_by_id
    from app.services.vector_adapter import process_vector_pipeline
    from app.services.sql_adapter import process_sql_pipeline
    
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided.")

    ext = Path(file.filename).suffix.lower()
    is_structured_upload = ext in (".csv", ".xlsx", ".xls")

    normalized_sql_mode = (sql_mode or "").strip().lower()
    normalized_table_name = (table_name or "").strip()

    if normalized_sql_mode and normalized_sql_mode not in ("auto", "otl", "inc"):
        raise HTTPException(status_code=400, detail="Invalid sql_mode. Use 'auto', 'otl', or 'inc'.")

    if is_structured_upload:
        if not normalized_sql_mode:
            normalized_sql_mode = "auto"
        if normalized_sql_mode in ("otl", "inc") and not normalized_table_name:
            raise HTTPException(status_code=400, detail="Structured CSV/XLSX upload requires table_name.")
    else:
        normalized_sql_mode = ""
        normalized_table_name = ""

    # Auto mode uses existing routing logic and does not override table name.
    if normalized_sql_mode == "auto":
        normalized_sql_mode = ""
        normalized_table_name = ""
    
    # Step 1: Upload
    content = await file.read()
    upload_result = await register_uploaded_file(
        content=content,
        original_filename=file.filename,
        source_type=source_type,
        source_identifier=source_identifier,
        source_url=source_url,
    )
    
    if not upload_result.success:
        if upload_result.is_duplicate:
            return {
                "success": False,
                "message": "File is a duplicate",
                "is_duplicate": True,
                "existing_file_id": upload_result.existing_file_id,
            }
        raise HTTPException(status_code=400, detail=upload_result.message)
    
    file_id = upload_result.file_id
    if not file_id:
        raise HTTPException(status_code=500, detail="Upload succeeded but file_id is missing.")
    
    # Step 2: Classify
    classification_result = await classify_file_by_id(
        file_id,
        sql_mode=normalized_sql_mode or None,
        table_name=normalized_table_name or None,
    )
    
    if not classification_result.get("success"):
        return {
            "success": False,
            "upload": {"file_id": file_id, "filename": upload_result.filename},
            "classification": classification_result,
            "processing": None,
        }
    
    # Step 3: Route to appropriate pipeline
    routed_to = classification_result.get("routed_to")

    # Manual SQL mode for structured files must be honored end-to-end.
    if is_structured_upload and normalized_sql_mode in ("otl", "inc"):
        routed_to = "sql_otl" if normalized_sql_mode == "otl" else "sql_inc"
        classification_result["routed_to"] = routed_to
    
    if routed_to == "vector_pipeline":
        # Unstructured -> Vector pipeline
        vector_result = await process_vector_pipeline(file_id)
        
        return {
            "success": vector_result.success,
            "upload": {"file_id": file_id, "filename": upload_result.filename},
            "classification": classification_result,
            "processing": {
                "pipeline": "vector",
                "success": vector_result.success,
                "vector_file_id": vector_result.vector_file_id,
                "chunks_created": vector_result.chunks_created,
                "embeddings_generated": vector_result.embeddings_generated,
                "tables_extracted": vector_result.tables_extracted,
                "error": vector_result.error,
            },
        }
    
    elif routed_to in ("sql_otl", "sql_inc"):
        # Structured -> SQL pipeline (SQL pipeline API)
        manual_sql_with_table = bool(normalized_table_name) and normalized_sql_mode in ("otl", "inc")
        sql_result = await process_sql_pipeline(
            file_id,
            preferred_table_name=normalized_table_name if manual_sql_with_table else None,
            skip_llm_table_name=manual_sql_with_table,
        )
        
        return {
            "success": sql_result.success,
            "upload": {"file_id": file_id, "filename": upload_result.filename},
            "classification": classification_result,
            "processing": {
                "pipeline": "sql",
                "success": sql_result.success,
                "sql_job_id": sql_result.sql_job_id,
                "sql_status": sql_result.sql_status,
                "preview": sql_result.preview,
                "error": sql_result.error,
                "note": "Approval required via SQL pipeline API" if sql_result.success else None,
                "approve_url": f"{settings.SQL_PIPELINE_API_URL}/approve/{sql_result.sql_job_id}" if sql_result.sql_job_id else None,
                "reject_url": f"{settings.SQL_PIPELINE_API_URL}/reject/{sql_result.sql_job_id}" if sql_result.sql_job_id else None,
            },
        }
    
    elif routed_to == "hitl":
        # Low confidence -> HITL queue
        return {
            "success": True,
            "upload": {"file_id": file_id, "filename": upload_result.filename},
            "classification": classification_result,
            "processing": {
                "pipeline": "hitl",
                "note": "File sent to HITL queue for human review due to low classification confidence.",
            },
        }
    
    else:
        return {
            "success": True,
            "upload": {"file_id": file_id, "filename": upload_result.filename},
            "classification": classification_result,
            "processing": None,
            "note": f"Unknown routing decision: {routed_to}",
        }


@app.post("/inbound/upload-classify-process/batch", tags=["Full Pipeline"])
async def upload_classify_and_process_batch(
    files: List[UploadFile] = File(...),
    source_type: SourceType = Form(default=SourceType.DIRECT_UPLOAD),
    source_identifier: Optional[str] = Form(default=None),
    source_url: Optional[str] = Form(default=None),
):
    """
    Batch end-to-end processing: Upload multiple files -> Classify -> Route -> Process in parallel.

    Files are registered sequentially (fast, DB-only), then classified and
    processed concurrently up to PARALLEL_FILE_LIMIT at a time.
    """
    import asyncio
    from app.services.classification_orchestrator import classify_file_by_id
    from app.services.vector_adapter import process_vector_pipeline
    from app.services.sql_adapter import process_sql_pipeline

    if not files:
        raise HTTPException(status_code=400, detail="No files provided.")

    # --- Phase 1: Register all files (sequential, fast) ----------------
    registered = []
    skipped = []

    for f in files:
        if not f.filename:
            skipped.append({"filename": "unknown", "reason": "No filename"})
            continue

        content = await f.read()
        upload_result = await register_uploaded_file(
            content=content,
            original_filename=f.filename,
            source_type=source_type,
            source_identifier=source_identifier,
            source_url=source_url,
        )

        if not upload_result.success:
            if upload_result.is_duplicate:
                skipped.append({
                    "filename": f.filename,
                    "reason": "duplicate",
                    "existing_file_id": upload_result.existing_file_id,
                })
            else:
                skipped.append({"filename": f.filename, "reason": upload_result.message})
            continue

        registered.append({
            "file_id": upload_result.file_id,
            "filename": upload_result.filename,
        })

    if not registered:
        return {
            "success": False,
            "message": "No new files registered (all duplicates or invalid).",
            "skipped": skipped,
            "results": [],
        }

    # --- Phase 2: Classify & process in parallel -----------------------
    sem = asyncio.Semaphore(settings.PARALLEL_FILE_LIMIT)

    async def _process_one(reg):
        async with sem:
            fid = reg["file_id"]
            fname = reg["filename"]

            # Classify
            cls_result = await classify_file_by_id(fid)
            if not cls_result.get("success"):
                return {
                    "file_id": fid,
                    "filename": fname,
                    "success": False,
                    "stage": "classification",
                    "error": cls_result.get("error"),
                }

            routed_to = cls_result.get("routed_to")

            # Process based on routing
            if routed_to == "vector_pipeline":
                vr = await process_vector_pipeline(fid)
                return {
                    "file_id": fid,
                    "filename": fname,
                    "success": vr.success,
                    "pipeline": "vector",
                    "vector_file_id": vr.vector_file_id,
                    "error": vr.error,
                }

            elif routed_to in ("sql_otl", "sql_inc"):
                sr = await process_sql_pipeline(fid)
                return {
                    "file_id": fid,
                    "filename": fname,
                    "success": sr.success,
                    "pipeline": "sql",
                    "sql_job_id": sr.sql_job_id,
                    "sql_status": sr.sql_status,
                    "error": sr.error,
                }

            elif routed_to == "hitl":
                return {
                    "file_id": fid,
                    "filename": fname,
                    "success": True,
                    "pipeline": "hitl",
                    "note": "Sent to HITL queue for human review",
                }

            else:
                return {
                    "file_id": fid,
                    "filename": fname,
                    "success": True,
                    "pipeline": "unknown",
                    "note": f"Unknown routing: {routed_to}",
                }

    results = await asyncio.gather(
        *[_process_one(reg) for reg in registered],
        return_exceptions=True,
    )

    # Convert exceptions to error dicts
    final_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final_results.append({
                "file_id": registered[i]["file_id"],
                "filename": registered[i]["filename"],
                "success": False,
                "error": str(r),
            })
        else:
            final_results.append(r)

    successful = sum(1 for r in final_results if r.get("success"))
    failed = sum(1 for r in final_results if not r.get("success"))

    return {
        "success": failed == 0,
        "message": f"Processed {len(final_results)} file(s). Successful: {successful}, Failed: {failed}, Skipped: {len(skipped)}",
        "total": len(final_results),
        "successful": successful,
        "failed": failed,
        "skipped_count": len(skipped),
        "skipped": skipped,
        "results": final_results,
    }


# ==========================================
# SQL Pipeline Endpoints
# ==========================================

@app.post("/inbound/files/{file_id}/process-sql", tags=["SQL Pipeline"])
async def process_file_sql(file_id: str):
    """
    Upload a file to the SQL pipeline for processing.
    
    The file must be:
    1. Already classified
    2. Routed to 'sql_otl' or 'sql_inc'
    
    This will upload the file to the SQL pipeline API and return the SQL job_id.
    Approval/rejection is handled via the SQL pipeline API directly:
    - POST {SQL_API}/approve/{job_id}
    - POST {SQL_API}/reject/{job_id}
    """
    from app.services.sql_adapter import process_sql_pipeline
    
    result = await process_sql_pipeline(file_id)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
    
    return {
        "success": True,
        "file_id": result.file_id,
        "sql_job_id": result.sql_job_id,
        "sql_status": result.sql_status,
        "preview": result.preview,
        "note": "Approval required via SQL pipeline API",
        "approve_url": f"{settings.SQL_PIPELINE_API_URL}/approve/{result.sql_job_id}",
        "reject_url": f"{settings.SQL_PIPELINE_API_URL}/reject/{result.sql_job_id}",
    }


@app.get("/inbound/files/{file_id}/sql-status", tags=["SQL Pipeline"])
async def get_sql_file_status(file_id: str):
    """
    Check the SQL pipeline status for a file.
    
    Syncs status between the SQL pipeline API and our inbound_files_metadata.
    """
    from app.services.sql_adapter import check_sql_job_completion
    
    result = await check_sql_job_completion(file_id)
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@app.post("/inbound/process-sql/all", tags=["SQL Pipeline"])
async def process_all_sql():
    """
    Upload all classified SQL files to the SQL pipeline.
    
    Processes files routed to sql_otl and sql_inc.
    """
    from app.services.sql_adapter import process_all_sql_files
    
    result = await process_all_sql_files()
    return {
        "success": True,
        "summary": {
            "total": result["total"],
            "successful": result["successful"],
            "failed": result["failed"],
        },
        "processed": result["processed"],
        "note": "Files uploaded to SQL pipeline. Approval required via SQL pipeline API.",
    }


# ==========================================
# Pipeline Stats Endpoint
# ==========================================

@app.get("/inbound/stats", tags=["Status"])
async def get_pipeline_stats():
    """
    Get overall pipeline statistics.
    """
    from app.models.db import get_db_pool

    pool = await get_db_pool()
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total_files,
                COUNT(*) FILTER (WHERE status = 'open_request') as open,
                COUNT(*) FILTER (WHERE status = 'classifying') as classifying,
                COUNT(*) FILTER (WHERE status = 'classified') as classified,
                COUNT(*) FILTER (WHERE status = 'processing') as processing,
                COUNT(*) FILTER (WHERE status = 'done') as done,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'hitl') as hitl,
                COUNT(*) FILTER (WHERE routed_to = 'vector_pipeline') as vector,
                COUNT(*) FILTER (WHERE routed_to LIKE 'sql_%') as sql
            FROM inbound_files_metadata;
            """
        )

    return {
        "success": True,
        "stats": dict(stats) if stats else {},
    }


# ==========================================
# Dashboard
# ==========================================

@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard_page():
    """Pipeline status dashboard."""
    html_path = Path(__file__).parent / "dashboard.html"
    return HTMLResponse(content=html_path.read_text(), status_code=200)


@app.get("/dashboard/data", tags=["Dashboard"])
async def dashboard_data():
    """JSON data feed for the dashboard."""
    import asyncpg

    dsn = settings.DATABASE_URL
    conn = await asyncpg.connect(dsn)

    try:
        # Inbound files
        rows = await conn.fetch("""
            SELECT
                CAST(id AS TEXT) AS id,
                source_type, source_identifier, source_url,
                original_filename, stored_filename, file_path,
                file_size, file_extension, mime_type,
                LEFT(sha256_hash, 16) AS sha256_hash,
                classification, classification_confidence,
                routed_to, operational_metadata_id,
                extracted_table_ids,
                CAST(vector_file_id AS TEXT) AS vector_file_id,
                status, error_message, retry_count,
                created_at, classification_completed_at,
                processing_started_at, processing_completed_at
            FROM inbound_files_metadata
            ORDER BY created_at DESC
        """)
        files = [dict(r) for r in rows]
        # Convert datetimes to ISO strings
        for f in files:
            for k in ('created_at', 'classification_completed_at',
                      'processing_started_at', 'processing_completed_at'):
                if f.get(k):
                    f[k] = f[k].isoformat()

        # Operational metadata (only the IDs referenced by our files + last 30)
        op_ids = [f['operational_metadata_id'] for f in files if f.get('operational_metadata_id')]
        if op_ids:
            placeholders = ','.join(f'${i+1}' for i in range(len(op_ids)))
            op_rows = await conn.fetch(f"""
                SELECT * FROM operational_metadata
                WHERE id IN ({placeholders})
                ORDER BY id DESC
            """, *op_ids)
        else:
            op_rows = []

        # Also get the latest 30 operational metadata entries (may overlap)
        recent_op = await conn.fetch("""
            SELECT * FROM operational_metadata
            ORDER BY id DESC LIMIT 30
        """)

        # Merge, deduplicate by id
        seen_ids = set()
        op_meta = []
        for r in list(op_rows) + list(recent_op):
            d = dict(r)
            if d['id'] not in seen_ids:
                seen_ids.add(d['id'])
                for k in ('last_updated_on', 'created_at', 'updated_at'):
                    if d.get(k):
                        d[k] = d[k].isoformat()
                op_meta.append(d)
        op_meta.sort(key=lambda x: x['id'], reverse=True)

        # SQL tables metadata (last 30 relevant)
        sql_rows = await conn.fetch("""
            SELECT * FROM tables_metadata
            ORDER BY updated_on DESC NULLS LAST
            LIMIT 30
        """)
        sql_meta = []
        for r in sql_rows:
            d = dict(r)
            for k in ('released_on', 'updated_on'):
                if d.get(k):
                    d[k] = d[k].isoformat()
            sql_meta.append(d)

        return {
            "files": files,
            "operational_metadata": op_meta,
            "tables_metadata": sql_meta,
        }
    finally:
        await conn.close()


@app.delete("/api/inbound/{file_id}", tags=["Dashboard"])
async def delete_inbound_record(file_id: str):
    """Delete one ingestion record from inbound_files_metadata by id (UUID)."""
    import asyncpg

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        result = await conn.execute(
            "DELETE FROM inbound_files_metadata WHERE id = $1::uuid RETURNING id",
            file_id,
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail=f"No record found for id {file_id}")
        return {"success": True, "deleted_id": file_id}
    finally:
        await conn.close()


@app.delete("/api/inbound/by-filename/{filename:path}", tags=["Dashboard"])
async def delete_inbound_record_by_filename(filename: str):
    """Delete ingestion record(s) from inbound_files_metadata by original_filename."""
    import asyncpg

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        deleted = await conn.fetch(
            "DELETE FROM inbound_files_metadata WHERE original_filename = $1 RETURNING id::text",
            filename,
        )
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"No record found for original_filename '{filename}'",
            )
        return {"success": True, "deleted_count": len(deleted), "deleted_ids": [r["id"] for r in deleted]}
    finally:
        await conn.close()


# ==========================================
# Approval Page + Proxy Endpoints
# ==========================================

@app.get("/upload", response_class=HTMLResponse, tags=["Dashboard"])
async def upload_page():
    """File upload page."""
    html_path = Path(__file__).parent / "upload.html"
    return HTMLResponse(content=html_path.read_text(), status_code=200)


@app.get("/approve", response_class=HTMLResponse, tags=["Dashboard"])
async def approval_page():
    """SQL pipeline approval/rejection page."""
    html_path = Path(__file__).parent / "approve.html"
    return HTMLResponse(content=html_path.read_text(), status_code=200)


@app.get("/approve/pending", tags=["Dashboard"])
async def get_pending_approvals():
    """Get all SQL files awaiting approval with their previews from SQL pipeline API."""
    import asyncpg

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        rows = await conn.fetch("""
            SELECT CAST(id AS TEXT) AS id, original_filename, file_extension,
                   file_size, routed_to, status, error_message,
                   classification, classification_confidence,
                   operational_metadata_id, created_at
            FROM inbound_files_metadata
            WHERE status = 'processing' AND routed_to LIKE 'sql_%'
            ORDER BY created_at DESC
        """)

        pending = []
        for r in rows:
            d = dict(r)
            d["created_at"] = d["created_at"].isoformat() if d.get("created_at") else None

            sql_job_id = None
            err = d.get("error_message", "")
            if err and err.startswith("sql_job_id:"):
                sql_job_id = err.replace("sql_job_id:", "")
            d["sql_job_id"] = sql_job_id

            # Fetch preview from SQL pipeline API
            d["preview"] = None
            d["sql_job_status"] = None
            pending.append(d)

        # Batch-fetch SQL job statuses (single client for all)
        if any(p.get("sql_job_id") for p in pending):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                    for p in pending:
                        job_id = p.get("sql_job_id")
                        if not job_id:
                            continue
                        try:
                            resp = await client.get(
                                f"{settings.SQL_PIPELINE_API_URL}/status/{job_id}"
                            )
                            if resp.status_code == 200:
                                api_data = resp.json()
                                p["sql_job_status"] = api_data.get("status")
                                p["preview"] = api_data.get("preview")
                                p["incremental_load_preview"] = api_data.get("incremental_load_preview")
                            elif resp.status_code == 404:
                                p["sql_job_status"] = "expired"
                            else:
                                p["sql_job_status"] = f"error_{resp.status_code}"
                        except Exception as e:
                            logger.warning(f"Failed to fetch SQL pipeline API status for {job_id}: {e}")
                            p["sql_job_status"] = "unreachable"
            except Exception as e:
                logger.error(f"SQL pipeline API client error: {e}")
                for p in pending:
                    if p.get("sql_job_id") and not p.get("sql_job_status"):
                        p["sql_job_status"] = "unreachable"

        return {"files": pending}
    finally:
        await conn.close()


@app.post("/approve/{file_id}/approve-sql", tags=["Dashboard"])
async def proxy_approve_sql(
    file_id: str,
    source: str = Form(...),
    source_url: str = Form(...),
    released_on: str = Form(...),
    updated_on: str = Form(...),
    table_name: Optional[str] = Form(default=None),
    business_metadata: Optional[str] = Form(default=None),
):
    """Approve a SQL job via SQL pipeline API and sync status back."""
    from app.services.sql_adapter import check_sql_job_completion

    file_record = await get_inbound_file(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    err = file_record.get("error_message", "")
    if not err or not err.startswith("sql_job_id:"):
        raise HTTPException(status_code=400, detail="No SQL job ID found for this file")

    sql_job_id = err.replace("sql_job_id:", "")

    # Forward approval to SQL pipeline API
    form_data = {
        "source": source,
        "source_url": source_url,
        "released_on": released_on,
        "updated_on": updated_on,
    }
    if table_name:
        form_data["table_name"] = table_name
    if business_metadata:
        form_data["business_metadata"] = business_metadata

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{settings.SQL_PIPELINE_API_URL}/approve/{sql_job_id}",
            data=form_data,
        )

    if resp.status_code != 200:
        # Job may already be completed; treat as success and return current status
        try:
            err_body = resp.json()
            detail_str = err_body.get("detail", "")
            if isinstance(detail_str, list):
                detail_str = " ".join(str(d) for d in detail_str)
            else:
                detail_str = str(detail_str or "")
            if "incremental_load_completed" in detail_str or ("completed" in detail_str and "not awaiting" in detail_str.lower()):
                async with httpx.AsyncClient(timeout=10) as status_client:
                    status_resp = await status_client.get(
                        f"{settings.SQL_PIPELINE_API_URL}/status/{sql_job_id}"
                    )
                if status_resp.status_code == 200:
                    sdata = status_resp.json()
                    final_status = sdata.get("status", "incremental_load_completed")
                    result_data = sdata.get("result")
                    sync_result = await check_sql_job_completion(file_id)
                    TERMINAL_SUCCESS = ("completed", "incremental_load_completed")
                    return {
                        "success": final_status in TERMINAL_SUCCESS,
                        "file_id": file_id,
                        "sql_job_id": sql_job_id,
                        "om_status": final_status,
                        "sql_job_status": final_status,
                        "table_name": sdata.get("result", {}).get("table_name") if isinstance(sdata.get("result"), dict) else None,
                        "result": result_data,
                        "sync": sync_result,
                    }
        except Exception:
            pass
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"SQL pipeline API returned {resp.status_code}: {resp.text}",
        )

    approve_result = resp.json()

    # Poll for completion (up to 30s)
    # SQL pipeline API may use "completed" or "incremental_load_completed" for success
    TERMINAL_SUCCESS = ("completed", "incremental_load_completed")
    import asyncio
    final_status = approve_result.get("status", "approved")
    result_data = None
    for _ in range(10):
        await asyncio.sleep(3)
        try:
            status_resp = await httpx.AsyncClient(timeout=10).get(
                f"{settings.SQL_PIPELINE_API_URL}/status/{sql_job_id}"
            )
            if status_resp.status_code == 200:
                sdata = status_resp.json()
                final_status = sdata.get("status", final_status)
                result_data = sdata.get("result")
                if final_status in (*TERMINAL_SUCCESS, "failed"):
                    break
        except Exception:
            break

    # If still "approved", incremental load may still be running; do one more check
    if final_status == "approved":
        await asyncio.sleep(2)
        try:
            status_resp = await httpx.AsyncClient(timeout=10).get(
                f"{settings.SQL_PIPELINE_API_URL}/status/{sql_job_id}"
            )
            if status_resp.status_code == 200:
                sdata = status_resp.json()
                final_status = sdata.get("status", final_status)
                result_data = sdata.get("result")
        except Exception:
            pass

    # Sync status to our DB
    sync_result = await check_sql_job_completion(file_id)

    # Treat incremental_load_completed as success (SQL pipeline uses this for IL completion)
    success = final_status in TERMINAL_SUCCESS
    return {
        "success": success,
        "file_id": file_id,
        "sql_job_id": sql_job_id,
        "om_status": final_status,
        "sql_job_status": final_status,
        "table_name": approve_result.get("table_name"),
        "result": result_data,
        "sync": sync_result,
    }


@app.post("/approve/{file_id}/reject-sql", tags=["Dashboard"])
async def proxy_reject_sql(file_id: str):
    """Reject a SQL job via SQL pipeline API and sync status back."""
    file_record = await get_inbound_file(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    err = file_record.get("error_message", "")
    if not err or not err.startswith("sql_job_id:"):
        raise HTTPException(status_code=400, detail="No SQL job ID found for this file")

    sql_job_id = err.replace("sql_job_id:", "")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{settings.SQL_PIPELINE_API_URL}/reject/{sql_job_id}"
        )

    # Update our DB
    await update_inbound_status(
        file_id=file_id,
        status="failed",
        error_message="SQL job rejected by user",
    )

    return {
        "success": True,
        "file_id": file_id,
        "sql_job_id": sql_job_id,
        "status": "rejected",
    }


# ==========================================
# HITL Classification Review Page + Endpoints
# ==========================================

@app.get("/hitl-review", response_class=HTMLResponse, tags=["Dashboard"])
async def hitl_review_page():
    """HITL classification review page."""
    html_path = Path(__file__).parent / "hitl_review.html"
    return HTMLResponse(content=html_path.read_text(), status_code=200)


@app.get("/hitl-review/pending", tags=["Dashboard"])
async def get_hitl_review_pending():
    """Get files routed to HITL that need classification review."""
    import asyncpg

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        rows = await conn.fetch("""
            SELECT CAST(id AS TEXT) AS id, original_filename, file_extension,
                   file_size, classification, classification_confidence,
                   status, created_at
            FROM inbound_files_metadata
            WHERE routed_to = 'hitl' AND status IN ('classified', 'hitl_required')
            ORDER BY created_at DESC
        """)

        files = []
        for r in rows:
            d = dict(r)
            d["created_at"] = d["created_at"].isoformat() if d.get("created_at") else None
            d["classification_confidence"] = float(d["classification_confidence"]) if d.get("classification_confidence") else None
            files.append(d)

        return {"files": files}
    finally:
        await conn.close()


@app.post("/hitl-review/{file_id}/process", tags=["Dashboard"])
async def hitl_review_process(file_id: str, pipeline: str = Form(...)):
    """
    Process a HITL file through the chosen pipeline.
    pipeline: "vector" or "sql"
    """
    from app.services.vector_adapter import process_vector_pipeline
    from app.services.sql_adapter import process_sql_pipeline
    import asyncpg

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        # Verify file exists and is in HITL state
        row = await conn.fetchrow(
            "SELECT status, routed_to FROM inbound_files_metadata WHERE id = $1::uuid",
            file_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="File not found")

        if pipeline == "vector":
            # Update routing to vector and process
            await conn.execute(
                """UPDATE inbound_files_metadata
                   SET routed_to = 'vector_pipeline', status = 'classified'
                   WHERE id = $1::uuid""",
                file_id,
            )
            await conn.close()
            conn = None

            result = await process_vector_pipeline(file_id)
            return {
                "success": result.success,
                "pipeline": "vector",
                "vector_file_id": result.vector_file_id,
                "error": result.error,
            }

        elif pipeline == "sql":
            # Update routing to SQL and process
            await conn.execute(
                """UPDATE inbound_files_metadata
                   SET routed_to = 'sql_otl', status = 'classified'
                   WHERE id = $1::uuid""",
                file_id,
            )
            await conn.close()
            conn = None

            result = await process_sql_pipeline(file_id)
            return {
                "success": result.success,
                "pipeline": "sql",
                "sql_job_id": result.sql_job_id,
                "sql_status": result.sql_status,
                "error": result.error,
            }

        else:
            raise HTTPException(status_code=400, detail="pipeline must be 'vector' or 'sql'")
    finally:
        if conn:
            await conn.close()


@app.post("/hitl-review/{file_id}/reject", tags=["Dashboard"])
async def hitl_review_reject(file_id: str):
    """Reject a HITL file."""
    import asyncpg

    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        await conn.execute("""
            UPDATE inbound_files_metadata
            SET status = 'failed', error_message = 'Rejected during HITL review'
            WHERE id = $1::uuid
        """, file_id)
        return {"success": True, "file_id": file_id}
    finally:
        await conn.close()


# ==========================================
# Main entry point
# ==========================================

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True,
    )
