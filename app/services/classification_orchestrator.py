"""
Classification Orchestrator Service

Coordinates the full classification flow:
1. Get file from database
2. Classify file (structured vs unstructured)
3. Read content sample for LLM
4. Extract metadata via LLM (domain, subdomain, summary)
5. Create operational_metadata entry
6. Update inbound_files_metadata with results

This service is called after file registration to determine
how the file should be processed.
"""
import aiofiles
from pathlib import Path
from typing import Dict, Any

from app.config import settings
from app.models.db import (
    get_inbound_file,
    update_inbound_status,
    insert_operational_metadata,
    exists_completed_sql_load_for_table,
)
from app.models.schemas import (
    FileStatus,
    Classification,
    RoutingDecision,
)
from app.services.classifier import classify_file
from app.services.metadata_extractor import extract_file_metadata


class ClassificationOrchestrator:
    """
    Orchestrates the full classification and metadata extraction flow.
    """

    def __init__(self):
        self.confidence_threshold = settings.CLASSIFICATION_CONFIDENCE_THRESHOLD

    async def classify_and_extract(
        self, file_id: str
    ) -> Dict[str, Any]:
        """
        Run full classification and metadata extraction for a file.

        Args:
            file_id: UUID of the file in inbound_files_metadata

        Returns:
            Dictionary with classification results and status
        """
        # Step 1: Get file record
        file_record = await get_inbound_file(file_id)
        if not file_record:
            return {
                "success": False,
                "error": f"File {file_id} not found",
            }

        # Check if already classified
        if file_record["status"] not in ("open_request", "failed"):
            return {
                "success": False,
                "error": f"File already in status: {file_record['status']}",
            }

        # Update status to classifying
        await update_inbound_status(file_id, FileStatus.CLASSIFYING.value)

        try:
            # Step 2: Classify file
            file_path = Path(file_record["file_path"])
            filename = file_record["original_filename"]
            mime_type = file_record.get("mime_type")

            # Read content for classification (if needed) and LLM
            content = None
            content_sample = ""

            if file_path.exists():
                async with aiofiles.open(file_path, "rb") as f:
                    content = await f.read()
                # Get text sample for LLM (first 4000 chars)
                content_sample = await self._get_content_sample(
                    file_path, content
                )

            classification_result = classify_file(  # noqa: E501
                filename=filename,
                mime_type=mime_type,
                content=content,
            )

            # Step 3: Extract metadata via LLM
            llm_metadata = await extract_file_metadata(
                filename=filename,
                content_sample=content_sample,
            )

            # Step 4: Determine routing (table_name needed for first vs subsequent)
            table_name = filename
            if classification_result.classification == Classification.STRUCTURED:
                table_name = Path(filename).stem
            routing = await self._determine_routing(
                classification_result, llm_metadata, table_name
            )

            # Step 5: Create operational_metadata entry
            # For PDFs, table_name stays as filename; for structured it's stem (set above)
            operational_id = await insert_operational_metadata(
                table_name=table_name,
                source_url=file_record.get("source_url"),
                major_domain=llm_metadata.major_domain,
                sub_domain=llm_metadata.sub_domain,
                brief_summary=llm_metadata.brief_summary,
                rows_count=None,  # Will be filled by processing pipeline
            )

            # Step 6: Update inbound_files_metadata
            await update_inbound_status(
                file_id=file_id,
                status=FileStatus.CLASSIFIED.value,
                classification=classification_result.classification.value,
                classification_confidence=classification_result.confidence,
                routed_to=routing.value,
                operational_metadata_id=operational_id,
            )

            return {
                "success": True,
                "file_id": file_id,
                "classification": classification_result.classification.value,
                "confidence": classification_result.confidence,
                "reason": classification_result.reason,
                "routed_to": routing.value,
                "operational_metadata_id": operational_id,
                "metadata": {
                    "major_domain": llm_metadata.major_domain,
                    "sub_domain": llm_metadata.sub_domain,
                    "brief_summary": llm_metadata.brief_summary,
                },
            }

        except Exception as e:
            # Update status to failed
            await update_inbound_status(
                file_id=file_id,
                status=FileStatus.FAILED.value,
                error_message=f"Classification failed: {str(e)}",
            )
            return {
                "success": False,
                "file_id": file_id,
                "error": str(e),
            }

    async def _get_content_sample(
        self,
        file_path: Path,
        content: bytes,
    ) -> str:
        """
        Get a text sample from file content for LLM analysis.

        For PDFs, we'd need OCR - for now, use filename.
        For text files, read first 4000 chars.
        """
        extension = file_path.suffix.lower()

        # For text-based files, decode directly
        if extension in (".txt", ".csv", ".json", ".xml"):
            try:
                text = content[:8000].decode("utf-8", errors="ignore")
                return text[:4000]
            except Exception:
                pass

        # For PDFs and other binary files, return placeholder
        # In production, this would use OCR (Mistral/Docling)
        if extension == ".pdf":
            return f"[PDF Document: {file_path.name}]"

        return f"[Binary file: {file_path.name}]"

    async def _determine_routing(
        self, classification_result, llm_metadata=None, table_name: str = ""
    ) -> RoutingDecision:
        """
        Determine routing from classification and, for structured files,
        from DB history: first load for table -> sql_otl, subsequent -> sql_inc.
        """
        # Low confidence -> HITL
        if classification_result.confidence < self.confidence_threshold:
            return RoutingDecision.HITL

        # Route based on classification
        if classification_result.classification == Classification.UNSTRUCTURED:
            return RoutingDecision.VECTOR_PIPELINE

        if classification_result.classification == Classification.STRUCTURED:
            # First load = no completed SQL load for this table (or parent); else incremental
            has_prior = await exists_completed_sql_load_for_table(table_name)
            return RoutingDecision.SQL_INC if has_prior else RoutingDecision.SQL_OTL

        # Unknown -> HITL
        return RoutingDecision.HITL


# Singleton instance
classification_orchestrator = ClassificationOrchestrator()


async def classify_file_by_id(file_id: str) -> Dict[str, Any]:
    """
    Convenience function to classify a file by ID.
    """
    return await classification_orchestrator.classify_and_extract(file_id)


async def classify_all_pending() -> Dict[str, Any]:
    """
    Classify all files with status 'open_request'.
    """
    from app.models.db import get_inbound_files

    pending_files = await get_inbound_files(status="open_request")

    results = []
    for file_record in pending_files:
        result = await classify_file_by_id(file_record["id"])
        results.append(result)

    return {
        "total": len(pending_files),
        "processed": len(results),
        "results": results,
    }
