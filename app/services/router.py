"""
Router Service

Orchestrates routing of classified files to the appropriate pipeline:
- Vector Pipeline (unstructured files)
- SQL Pipeline (structured files)
- HITL Queue (low confidence / unknown)

This is the main entry point after classification.
"""
from typing import Dict, Any

from app.models.db import get_inbound_file, update_inbound_status
from app.models.schemas import FileStatus, RoutingDecision
from app.services.vector_adapter import handoff_to_vector
from app.services.sql_adapter import handoff_to_sql
from app.services.hitl import escalate_to_hitl, HITLReason


class Router:
    """
    Routes classified files to appropriate processing pipelines.
    """

    async def route_file(self, file_id: str) -> Dict[str, Any]:
        """
        Route a classified file to the appropriate pipeline.

        Args:
            file_id: UUID of the file to route

        Returns:
            Dict with routing result
        """
        # Get file record
        file_record = await get_inbound_file(file_id)
        if not file_record:
            return {"success": False, "error": "File not found"}

        # Check status
        if file_record["status"] != FileStatus.CLASSIFIED.value:
            return {
                "success": False,
                "error": f"File not ready for routing. Status: {file_record['status']}",
            }

        routed_to = file_record.get("routed_to")
        if not routed_to:
            return {"success": False, "error": "No routing decision found"}

        # Route based on decision
        if routed_to == RoutingDecision.VECTOR_PIPELINE.value:
            return await self._route_to_vector(file_id, file_record)

        elif routed_to in (
            RoutingDecision.SQL_OTL.value,
            RoutingDecision.SQL_INC.value
        ):
            return await self._route_to_sql(file_id, file_record)

        elif routed_to == RoutingDecision.HITL.value:
            return await self._route_to_hitl(file_id, file_record)

        else:
            return {
                "success": False,
                "error": f"Unknown routing decision: {routed_to}",
            }

    async def _route_to_vector(
        self, file_id: str, file_record: Dict
    ) -> Dict[str, Any]:
        """
        Route file to vector pipeline.
        """
        print(f"[ROUTER] Routing {file_id} to Vector Pipeline")

        result = await handoff_to_vector(file_id)

        return {
            "success": True,
            "file_id": file_id,
            "routed_to": "vector_pipeline",
            "handoff_result": result,
            "message": "File handed off to vector pipeline",
        }

    async def _route_to_sql(
        self, file_id: str, file_record: Dict
    ) -> Dict[str, Any]:
        """
        Route file to SQL pipeline.
        """
        print(f"[ROUTER] Routing {file_id} to SQL Pipeline")

        result = await handoff_to_sql(file_id)

        return {
            "success": True,
            "file_id": file_id,
            "routed_to": "sql_pipeline",
            "handoff_result": result,
            "message": "File handed off to SQL pipeline",
        }

    async def _route_to_hitl(
        self, file_id: str, file_record: Dict
    ) -> Dict[str, Any]:
        """
        Route file to HITL queue.
        """
        print(f"[ROUTER] Routing {file_id} to HITL Queue")

        # Determine reason
        confidence = file_record.get("classification_confidence", 0)
        if confidence < 0.8:
            reason = HITLReason.LOW_CONFIDENCE
            details = f"Classification confidence {confidence:.2f} below threshold"
        else:
            reason = HITLReason.UNKNOWN_TYPE
            details = "Routed to HITL by classifier"

        hitl_id = await escalate_to_hitl(
            file_id=file_id,
            reason=reason,
            reason_details=details,
            original_classification=file_record.get("classification"),
            original_confidence=confidence,
        )

        return {
            "success": True,
            "file_id": file_id,
            "routed_to": "hitl",
            "hitl_id": hitl_id,
            "message": "File escalated to HITL queue for human review",
        }


# Singleton instance
router = Router()


async def route_file(file_id: str) -> Dict[str, Any]:
    """
    Convenience function to route a file.
    """
    return await router.route_file(file_id)


async def route_all_classified() -> Dict[str, Any]:
    """
    Route all files with status 'classified'.
    """
    from app.models.db import get_inbound_files

    classified_files = await get_inbound_files(status="classified")

    results = []
    for file_record in classified_files:
        result = await route_file(file_record["id"])
        results.append(result)

    return {
        "total": len(classified_files),
        "routed": len(results),
        "results": results,
    }
