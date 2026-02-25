"""
Pydantic models for request/response schemas
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


class FileStatus(str, Enum):
    """Status values for inbound files."""
    OPEN_REQUEST = "open_request"
    CLASSIFYING = "classifying"
    CLASSIFIED = "classified"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"
    HITL = "hitl"
    DUPLICATE = "duplicate"


class Classification(str, Enum):
    """File classification types."""
    STRUCTURED = "structured"
    UNSTRUCTURED = "unstructured"
    UNKNOWN = "unknown"


class RoutingDecision(str, Enum):
    """Routing decisions for files."""
    VECTOR_PIPELINE = "vector_pipeline"
    SQL_OTL = "sql_otl"
    SQL_INC = "sql_inc"
    HITL = "hitl"


class SourceType(str, Enum):
    """Source types for incoming files."""
    AGENT_INBOX = "agent_inbox"
    ACQ_AGENT = "acq_agent"
    ONDEMAND = "ondemand"
    DATA_API = "data_api"
    DIRECT_UPLOAD = "direct_upload"


# ==========================================
# Inbound Files Metadata Schemas
# ==========================================

class InboundFileCreate(BaseModel):
    """Schema for creating a new inbound file entry."""
    source_type: SourceType = SourceType.DIRECT_UPLOAD
    source_identifier: Optional[str] = None
    source_url: Optional[str] = None
    original_filename: str
    stored_filename: str
    file_path: str
    file_size: int
    file_extension: str
    mime_type: Optional[str] = None
    sha256_hash: str


class InboundFileUpdate(BaseModel):
    """Schema for updating an inbound file entry."""
    classification: Optional[Classification] = None
    classification_confidence: Optional[float] = None
    routed_to: Optional[RoutingDecision] = None
    operational_metadata_id: Optional[int] = None
    extracted_table_ids: Optional[List[int]] = None
    vector_file_id: Optional[str] = None
    status: Optional[FileStatus] = None
    error_message: Optional[str] = None
    retry_count: Optional[int] = None


class InboundFileResponse(BaseModel):
    """Response schema for inbound file."""
    id: str
    source_type: str
    source_identifier: Optional[str]
    source_url: Optional[str]
    original_filename: str
    stored_filename: str
    file_path: str
    file_size: int
    file_extension: str
    mime_type: Optional[str]
    sha256_hash: str
    classification: Optional[str]
    classification_confidence: Optional[float]
    routed_to: Optional[str]
    operational_metadata_id: Optional[int]
    extracted_table_ids: Optional[List[int]]
    vector_file_id: Optional[str]
    status: str
    error_message: Optional[str]
    retry_count: int
    created_at: datetime
    classification_completed_at: Optional[datetime]
    processing_started_at: Optional[datetime]
    processing_completed_at: Optional[datetime]

    class Config:
        from_attributes = True


class InboundFileStatus(BaseModel):
    """Lightweight status response."""
    id: str
    status: str
    updated_at: datetime


# ==========================================
# Classification Schemas
# ==========================================

class ClassificationResult(BaseModel):
    """Result from file classification."""
    classification: Classification
    confidence: float = Field(ge=0.0, le=1.0)
    file_extension: str
    mime_type: Optional[str]
    reason: Optional[str] = None


class LLMMetadataResult(BaseModel):
    """Result from LLM metadata extraction."""
    major_domain: str
    sub_domain: str
    brief_summary: str
    load_type: Optional[str] = "one_time"  # "one_time" | "incremental"


# ==========================================
# Operational Metadata Schemas
# ==========================================

class OperationalMetadataCreate(BaseModel):
    """Schema for creating operational metadata entry."""
    table_name: str  # For PDFs, this is the filename
    table_view: Optional[str] = None
    source_url: Optional[str] = None
    major_domain: Optional[str] = None
    sub_domain: Optional[str] = None
    brief_summary: Optional[str] = None
    rows_count: Optional[int] = None
    columns: Optional[str] = None
    period_cols: Optional[str] = None
    first_available_value: Optional[str] = None
    last_available_value: Optional[str] = None
    business_metadata: Optional[str] = None


class OperationalMetadataUpdate(BaseModel):
    """Schema for updating operational metadata."""
    major_domain: Optional[str] = None
    sub_domain: Optional[str] = None
    brief_summary: Optional[str] = None
    rows_count: Optional[int] = None
    columns: Optional[str] = None
    period_cols: Optional[str] = None
    first_available_value: Optional[str] = None
    last_available_value: Optional[str] = None
    business_metadata: Optional[str] = None


# ==========================================
# API Request/Response Schemas
# ==========================================

class UploadResponse(BaseModel):
    """Response for file upload."""
    success: bool
    message: str
    file_id: Optional[str] = None
    filename: Optional[str] = None
    status: Optional[str] = None
    is_duplicate: bool = False
    existing_file_id: Optional[str] = None


class BatchUploadResponse(BaseModel):
    """Response for batch file upload."""
    success: bool
    message: str
    total_files: int
    successful: int
    duplicates: int
    failed: int
    files: List[UploadResponse]
