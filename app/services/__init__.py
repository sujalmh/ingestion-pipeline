# Services module
from .registration import (
    RegistrationService,
    registration_service,
    register_uploaded_file,
)
from .classifier import (
    FileClassifier,
    file_classifier,
    classify_file,
)
from .metadata_extractor import (
    MetadataExtractor,
    metadata_extractor,
    extract_file_metadata,
)
from .classification_orchestrator import (
    ClassificationOrchestrator,
    classification_orchestrator,
    classify_file_by_id,
    classify_all_pending,
)
from .router import (
    Router,
    router,
    route_file,
    route_all_classified,
)
from .vector_adapter import (
    VectorPipelineResult,
    handoff_to_vector,
    process_vector_pipeline,
    process_all_vector_files,
)
from .sql_adapter import (
    SQLPipelineResult,
    handoff_to_sql,
    process_sql_pipeline,
    process_all_sql_files,
)
from .hitl import (
    escalate_to_hitl,
    get_hitl_queue,
    get_hitl_item,
    approve_hitl_item,
    reject_hitl_item,
    get_hitl_stats,
    HITLStatus,
    HITLReason,
)
