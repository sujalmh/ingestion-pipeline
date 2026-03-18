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
    SourceCreate,
    SourceUpdate,
    APIEndpointCreate,
    TestFetchRequest,
    TestFetchResponse,
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
    
    # Create agent downloads directory
    agent_dir = upload_dir / "agent_downloads"
    agent_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Agent downloads directory: {agent_dir.resolve()}")
    
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
            preferred_sql_mode=normalized_sql_mode or None,
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


@app.post("/approve/batch-approve-sql", tags=["Dashboard"])
async def batch_approve_sql(
    source: str = Form(...),
    source_url: str = Form(...),
    released_on: str = Form(...),
    updated_on: str = Form(...),
    business_metadata: Optional[str] = Form(default=None),
):
    """
    Batch-approve all pending SQL jobs with shared metadata.

    Forwards to the SQL pipeline API's /batch-approve endpoint, then
    syncs completion status for each approved job back to our DB.
    """
    from app.services.sql_adapter import check_sql_job_completion
    import asyncio, asyncpg

    form_data = {
        "source": source,
        "source_url": source_url,
        "released_on": released_on,
        "updated_on": updated_on,
    }
    if business_metadata:
        form_data["business_metadata"] = business_metadata

    # Forward to SQL pipeline API
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            resp = await client.post(
                f"{settings.SQL_PIPELINE_API_URL}/batch-approve",
                data=form_data,
            )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                "SQL pipeline batch-approve timed out after 60 seconds. "
                "The SQL pipeline may still have accepted the request. "
                "Run: python scripts/recover_stuck_sql_processing.py --apply --approve-individual "
                "--source '...' --source-url '...' --released-on '...' --updated-on '...'"
            ),
        ) from exc
    except httpx.TransportError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Cannot reach SQL pipeline API: {exc}",
        ) from exc

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"SQL pipeline batch-approve returned {resp.status_code}: {resp.text[:500]}",
        )

    try:
        batch_result = resp.json()
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"SQL pipeline batch-approve returned non-JSON response: {resp.text[:300]}",
        ) from exc
    approved_jobs = batch_result.get("approved", [])

    # Sync each approved job's completion status back to our DB
    if approved_jobs:
        conn = await asyncpg.connect(settings.DATABASE_URL)
        try:
            for job_info in approved_jobs:
                sql_job_id = job_info.get("job_id")
                if not sql_job_id:
                    continue
                # Find the inbound file that owns this sql_job_id
                row = await conn.fetchrow(
                    """SELECT CAST(id AS TEXT) AS id FROM inbound_files_metadata
                       WHERE error_message = $1 LIMIT 1""",
                    f"sql_job_id:{sql_job_id}",
                )
                if row:
                    file_id = row["id"]
                    # Background: poll for completion then sync
                    asyncio.create_task(_sync_batch_job(file_id, sql_job_id))
        finally:
            await conn.close()

    return batch_result


async def _sync_batch_job(file_id: str, sql_job_id: str):
    """Poll SQL API for a batch-approved job and sync status to our DB."""
    import asyncio
    from app.services.sql_adapter import check_sql_job_completion

    TERMINAL = ("completed", "incremental_load_completed", "failed")
    for _ in range(12):
        await asyncio.sleep(5)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"{settings.SQL_PIPELINE_API_URL}/status/{sql_job_id}"
                )
            if resp.status_code == 200:
                status = resp.json().get("status")
                if status in TERMINAL:
                    await check_sql_job_completion(file_id)
                    return
        except Exception as e:
            logger.warning(f"Batch sync poll error for {sql_job_id}: {e}")
    # Final sync attempt
    try:
        await check_sql_job_completion(file_id)
    except Exception:
        pass


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
async def hitl_review_process(
    file_id: str,
    pipeline: str = Form(...),
    sql_mode: str = Form("otl"),
):
    """
    Process a HITL file through the chosen pipeline.
    pipeline: "vector" or "sql"
    sql_mode: "otl" or "inc" when pipeline == "sql"
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
            normalized_sql_mode = (sql_mode or "otl").strip().lower()
            if normalized_sql_mode not in ("otl", "inc"):
                raise HTTPException(status_code=400, detail="Invalid sql_mode. Use 'otl' or 'inc'.")

            routed_to = "sql_inc" if normalized_sql_mode == "inc" else "sql_otl"

            # Update routing to the selected SQL mode and process
            await conn.execute(
                """UPDATE inbound_files_metadata
                   SET routed_to = $2, status = 'classified'
                   WHERE id = $1::uuid""",
                file_id,
                routed_to,
            )
            await conn.close()
            conn = None

            result = await process_sql_pipeline(file_id, preferred_sql_mode=normalized_sql_mode)
            return {
                "success": result.success,
                "pipeline": "sql",
                "sql_mode": normalized_sql_mode,
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
# Data API Agent Endpoints
# ==========================================

@app.post("/agent/run", tags=["Data API Agent"])
async def agent_run_all():
    """Run all configured and enabled data sources."""
    from app.agent.orchestrator import run_agent_all
    result = await run_agent_all()
    return result


@app.post("/agent/run/{source_id}", tags=["Data API Agent"])
async def agent_run_source(source_id: str):
    """Run all APIs for a specific data source."""
    from app.agent.orchestrator import run_agent_source
    result = await run_agent_source(source_id)
    if not result.get("success") and "not found" in result.get("error", ""):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.post("/agent/run/{source_id}/{api_id}", tags=["Data API Agent"])
async def agent_run_single_api(source_id: str, api_id: str):
    """Run a single API endpoint from a data source."""
    from app.agent.orchestrator import run_agent_single_api
    result = await run_agent_single_api(source_id, api_id)
    if not result.get("success") and "not found" in result.get("error", ""):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@app.get("/agent/status", tags=["Data API Agent"])
async def agent_status(source_id: Optional[str] = Query(default=None), limit: int = Query(default=20, le=100)):
    """Get agent run history and status."""
    from app.models.db import get_agent_runs
    runs = await get_agent_runs(source_id=source_id, limit=limit)
    # Serialize datetimes
    for run in runs:
        for k in ("started_at", "completed_at"):
            if run.get(k):
                run[k] = run[k].isoformat()
    return {"success": True, "runs": runs}


# ==========================================
# Source Registry Management Endpoints
# ==========================================

@app.get("/agent/sources", tags=["Source Registry"])
async def list_sources():
    """List all configured sources (YAML + DB merged)."""
    from app.agent.source_config import get_all_sources
    configs = await get_all_sources()

    sources = []
    for sid, cfg in sorted(configs.items()):
        sources.append({
            "source_id": cfg.source_id,
            "name": cfg.name,
            "base_url": cfg.base_url,
            "auth_type": cfg.auth.type,
            "enabled": cfg.enabled,
            "origin": cfg.origin,
            "editable": cfg.origin == "db",
            "api_count": len(cfg.apis),
            "apis": [
                {
                    "api_id": a.id,
                    "name": a.name,
                    "endpoint": a.endpoint,
                    "method": a.method,
                    "schedule": a.schedule,
                    "enabled": a.enabled,
                    "output_format": a.output_format,
                    "period": a.period.model_dump() if a.period else None,
                }
                for a in cfg.apis
            ],
        })

    return {"success": True, "total": len(sources), "sources": sources}


@app.get("/agent/sources/{source_id}", tags=["Source Registry"])
async def get_source_detail(source_id: str):
    """Get details of a specific source."""
    from app.agent.source_config import get_source
    cfg = await get_source(source_id)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    return {
        "success": True,
        "source": {
            "source_id": cfg.source_id,
            "name": cfg.name,
            "base_url": cfg.base_url,
            "auth_type": cfg.auth.type,
            "enabled": cfg.enabled,
            "origin": cfg.origin,
            "editable": cfg.origin == "db",
            "apis": [
                {
                    "api_id": a.id,
                    "name": a.name,
                    "endpoint": a.endpoint,
                    "method": a.method,
                    "description": a.description,
                    "output_format": a.output_format,
                    "params": a.params,
                    "headers": a.headers,
                    "schedule": a.schedule,
                    "enabled": a.enabled,
                    "metadata": a.metadata,
                    "period": a.period.model_dump() if a.period else None,
                }
                for a in cfg.apis
            ],
        },
    }


@app.post("/agent/sources", tags=["Source Registry"])
async def register_source(payload: SourceCreate):
    """
    Register a new API source with its endpoints.
    Only creates DB-backed sources (YAML sources are managed via config files).
    """
    from app.agent.source_config import get_source_config
    from app.models.db import insert_source

    # Block if source_id conflicts with a YAML source
    yaml_cfg = get_source_config(payload.source_id)
    if yaml_cfg:
        raise HTTPException(
            status_code=409,
            detail=f"Source '{payload.source_id}' already exists as a YAML config and cannot be overridden via API.",
        )

    source_data = {
        "source_id": payload.source_id,
        "name": payload.name,
        "base_url": payload.base_url,
        "auth_type": payload.auth_type.value,
        "auth_login_url": payload.auth_login_url,
        "auth_token_expiry_minutes": payload.auth_token_expiry_minutes or 14,
        "auth_credentials": payload.auth_credentials or {},
        "default_metadata": payload.default_metadata or {},
        "apis": [
            {
                "api_id": api.api_id,
                "name": api.name,
                "endpoint": api.endpoint,
                "method": api.method,
                "description": api.description,
                "output_format": api.output_format,
                "params": api.params or {},
                "headers": api.headers or {},
                "schedule": api.schedule,
                "metadata": api.metadata or {},
                "period_config": api.period.model_dump() if api.period else None,
            }
            for api in payload.apis
        ],
    }

    try:
        sid = await insert_source(source_data)
        return {
            "success": True,
            "message": f"Source '{sid}' registered with {len(payload.apis)} API(s)",
            "source_id": sid,
            "api_count": len(payload.apis),
        }
    except Exception as e:
        if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"Source '{payload.source_id}' already exists")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/agent/sources/{source_id}", tags=["Source Registry"])
async def update_source_endpoint(source_id: str, payload: SourceUpdate):
    """Update a DB-registered source. YAML sources cannot be edited via API."""
    from app.agent.source_config import get_source_config
    from app.models.db import update_source as db_update_source

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(
            status_code=403,
            detail="This source is managed by YAML config and cannot be edited via API.",
        )

    updates = payload.model_dump(exclude_none=True)
    if "auth_type" in updates:
        updates["auth_type"] = updates["auth_type"].value if hasattr(updates["auth_type"], "value") else updates["auth_type"]

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    success = await db_update_source(source_id, updates)
    if not success:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found in DB registry")

    return {"success": True, "source_id": source_id, "updated_fields": list(updates.keys())}


@app.delete("/agent/sources/{source_id}", tags=["Source Registry"])
async def delete_source_endpoint(source_id: str):
    """Delete a DB-registered source (cascades to its endpoints). YAML sources cannot be deleted."""
    from app.agent.source_config import get_source_config
    from app.models.db import delete_source as db_delete_source

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(
            status_code=403,
            detail="This source is managed by YAML config and cannot be deleted via API.",
        )

    success = await db_delete_source(source_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    return {"success": True, "message": f"Source '{source_id}' and all its endpoints deleted"}


@app.patch("/agent/sources/{source_id}/toggle", tags=["Source Registry"])
async def toggle_source_endpoint(source_id: str, enabled: bool = Query(...)):
    """Enable or disable a DB-registered source."""
    from app.agent.source_config import get_source_config
    from app.models.db import toggle_source as db_toggle_source

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(status_code=403, detail="Cannot toggle YAML sources via API.")

    success = await db_toggle_source(source_id, enabled)
    if not success:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    return {"success": True, "source_id": source_id, "enabled": enabled}


# --- Endpoint-level management ---

@app.post("/agent/sources/{source_id}/apis", tags=["Source Registry"])
async def add_endpoint(source_id: str, payload: APIEndpointCreate):
    """Add an API endpoint to an existing DB source."""
    from app.agent.source_config import get_source_config
    from app.models.db import add_api_endpoint, get_db_source

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(status_code=403, detail="Cannot add endpoints to YAML sources via API.")

    db_src = await get_db_source(source_id)
    if not db_src:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found in DB registry")

    try:
        ep_id = await add_api_endpoint(source_id, payload.model_dump())
        return {"success": True, "source_id": source_id, "api_id": payload.api_id, "endpoint_record_id": ep_id}
    except Exception as e:
        if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail=f"API '{payload.api_id}' already exists in source '{source_id}'")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/agent/sources/{source_id}/apis/{api_id}", tags=["Source Registry"])
async def update_endpoint(source_id: str, api_id: str, payload: dict):
    """Update an API endpoint in a DB source."""
    from app.agent.source_config import get_source_config
    from app.models.db import update_api_endpoint

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(status_code=403, detail="Cannot edit endpoints on YAML sources via API.")

    success = await update_api_endpoint(source_id, api_id, payload)
    if not success:
        raise HTTPException(status_code=404, detail=f"Endpoint '{api_id}' not found in source '{source_id}'")

    return {"success": True, "source_id": source_id, "api_id": api_id}


@app.delete("/agent/sources/{source_id}/apis/{api_id}", tags=["Source Registry"])
async def remove_endpoint(source_id: str, api_id: str):
    """Delete an API endpoint from a DB source."""
    from app.agent.source_config import get_source_config
    from app.models.db import delete_api_endpoint

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(status_code=403, detail="Cannot delete endpoints on YAML sources via API.")

    success = await delete_api_endpoint(source_id, api_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Endpoint '{api_id}' not found")

    return {"success": True, "source_id": source_id, "api_id": api_id, "message": "Endpoint deleted"}


@app.patch("/agent/sources/{source_id}/apis/{api_id}/toggle", tags=["Source Registry"])
async def toggle_api_endpoint(source_id: str, api_id: str, enabled: bool = Query(...)):
    """Enable or disable an API endpoint."""
    from app.agent.source_config import get_source_config
    from app.models.db import toggle_endpoint

    yaml_cfg = get_source_config(source_id)
    if yaml_cfg:
        raise HTTPException(status_code=403, detail="Cannot toggle endpoints on YAML sources via API.")

    success = await toggle_endpoint(source_id, api_id, enabled)
    if not success:
        raise HTTPException(status_code=404, detail=f"Endpoint '{api_id}' not found")

    return {"success": True, "source_id": source_id, "api_id": api_id, "enabled": enabled}


# --- Test fetch ---

@app.post("/agent/sources/test", tags=["Source Registry"])
async def test_fetch_endpoint(payload: TestFetchRequest):
    """
    Test-fetch an API endpoint without saving.
    Validates the API by making a real request and returns a preview.
    """
    from app.agent.fetcher import test_fetch_api

    result = await test_fetch_api(
        base_url=payload.base_url,
        endpoint=payload.endpoint,
        method=payload.method,
        params=payload.params or {},
        headers=payload.headers or {},
        auth_type=payload.auth_type.value,
        auth_login_url=payload.auth_login_url,
        auth_credentials=payload.auth_credentials or {},
    )

    return result


# --- Source Management UI ---

@app.get("/agent/manage", response_class=HTMLResponse, tags=["Source Registry"])
async def source_management_page():
    """Source management dashboard for business users."""
    html_path = Path(__file__).parent / "source_manage.html"
    if not html_path.exists():
        return HTMLResponse(
            content="<h1>Source management page not found</h1>",
            status_code=404,
        )
    return HTMLResponse(content=html_path.read_text(), status_code=200)


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
