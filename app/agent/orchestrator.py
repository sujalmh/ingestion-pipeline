"""
Agent Orchestrator — Main agent coordinator.

Orchestrates the full data API fetch + pipeline ingestion flow:
load config → authenticate → fetch → convert → feed pipeline → log to DB.

Pattern: Class + singleton + convenience functions (like ClassificationOrchestrator)
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from app.config import settings

logger = logging.getLogger(__name__)


# ============ Orchestrator Class ============

class AgentOrchestrator:
    """Orchestrates data API fetching and pipeline ingestion."""

    def __init__(self):
        self.output_dir = Path(settings.UPLOAD_DIRECTORY) / "agent_downloads"

    async def run_source(self, source_id: str) -> Dict[str, Any]:
        """
        Run all APIs for a specific source.

        Args:
            source_id: The source identifier.

        Returns:
            Dict with run results.
        """
        from app.agent.source_config import get_source
        from app.agent.fetcher import fetch_all_apis
        from app.agent.converter import convert_to_file
        from app.agent.pipeline_feeder import feed_all

        config = await get_source(source_id)
        if not config:
            return {
                "success": False,
                "source_id": source_id,
                "error": f"Source '{source_id}' not found in YAML configs or DB registry",
            }

        if not config.enabled:
            return {
                "success": False,
                "source_id": source_id,
                "error": f"Source '{source_id}' is disabled",
            }

        # Log run start
        run_id = await self._log_run_start(source_id)
        started_at = datetime.now()

        try:
            # Step 1: Fetch all APIs
            logger.info(f"Agent run for source: {source_id} ({len(config.apis)} APIs)")
            fetch_results = await fetch_all_apis(config)

            # Step 2: Convert successful fetches to files
            converted_files = []
            fetch_errors = []

            for fr in fetch_results:
                if not fr.success:
                    fetch_errors.append({
                        "api_id": fr.api_id,
                        "error": fr.error,
                    })
                    continue

                # Find matching API config
                api_config = next((a for a in config.apis if a.id == fr.api_id), None)
                if not api_config:
                    continue

                # Build period label for filename traceability
                period_label = self._get_period_label(source_id, fr.api_id)

                conv_result = await convert_to_file(
                    data=fr.data,
                    source_id=source_id,
                    api_config=api_config,
                    output_dir=str(self.output_dir),
                    content_type=fr.content_type,
                    period_label=period_label,
                )

                if conv_result.success:
                    converted_files.append({
                        "file_path": conv_result.file_path,
                        "filename": conv_result.filename,
                        "api_config": api_config,
                        "metadata": conv_result.metadata,
                    })
                    # Persist period watermark if this was a period-aware fetch
                    await self._save_period_watermark(source_id, fr.api_id)
                else:
                    fetch_errors.append({
                        "api_id": fr.api_id,
                        "error": f"Conversion failed: {conv_result.error}",
                    })

            # Step 3: Feed through pipeline
            feed_result = {"total": 0, "ingested": 0, "skipped": 0, "failed": 0, "results": []}
            if converted_files:
                feed_result = await feed_all(converted_files, config)

            # Step 4: Log run completion
            result = {
                "success": True,
                "source_id": source_id,
                "source_name": config.name,
                "apis_total": len(config.apis),
                "apis_fetched": len(fetch_results) - len(fetch_errors),
                "apis_failed": len(fetch_errors),
                "files_converted": len(converted_files),
                "files_ingested": feed_result["ingested"],
                "files_skipped": feed_result["skipped"],
                "files_failed": feed_result["failed"],
                "fetch_errors": fetch_errors,
                "pipeline_results": feed_result["results"],
                "duration_seconds": (datetime.now() - started_at).total_seconds(),
            }

            await self._log_run_complete(run_id, result)
            logger.info(
                f"Agent run completed for {source_id}: "
                f"fetched={result['apis_fetched']}, "
                f"ingested={result['files_ingested']}, "
                f"skipped={result['files_skipped']}"
            )
            return result

        except Exception as e:
            error_msg = f"Agent run failed for {source_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            await self._log_run_error(run_id, error_msg)
            return {
                "success": False,
                "source_id": source_id,
                "error": error_msg,
                "duration_seconds": (datetime.now() - started_at).total_seconds(),
            }

    async def run_all(self) -> Dict[str, Any]:
        """
        Run all configured and enabled sources.

        Returns:
            Dict with aggregate results.
        """
        from app.agent.source_config import get_all_sources

        configs = await get_all_sources()
        enabled = {sid: cfg for sid, cfg in configs.items() if cfg.enabled}

        if not enabled:
            return {
                "success": True,
                "message": "No enabled sources configured",
                "sources_total": len(configs),
                "sources_enabled": 0,
                "results": [],
            }

        results = []
        for source_id in enabled:
            result = await self.run_source(source_id)
            results.append(result)

        successful = sum(1 for r in results if r.get("success"))
        failed = sum(1 for r in results if not r.get("success"))

        return {
            "success": failed == 0,
            "sources_total": len(configs),
            "sources_enabled": len(enabled),
            "sources_successful": successful,
            "sources_failed": failed,
            "results": results,
        }

    async def run_single_api(self, source_id: str, api_id: str) -> Dict[str, Any]:
        """
        Run a single API endpoint from a source.

        Args:
            source_id: The source identifier.
            api_id: The API endpoint identifier.

        Returns:
            Dict with run results.
        """
        from app.agent.source_config import get_source
        from app.agent.fetcher import fetch_api
        from app.agent.converter import convert_to_file
        from app.agent.pipeline_feeder import feed_file

        config = await get_source(source_id)
        if not config:
            return {
                "success": False,
                "error": f"Source '{source_id}' not found",
            }

        api_config = next((a for a in config.apis if a.id == api_id), None)
        if not api_config:
            available = [a.id for a in config.apis]
            return {
                "success": False,
                "error": f"API '{api_id}' not found in source '{source_id}'. Available: {available}",
            }

        # Log run start
        run_id = await self._log_run_start(source_id, api_id)
        started_at = datetime.now()

        try:
            # Fetch
            fr = await fetch_api(config, api_config)
            if not fr.success:
                await self._log_run_error(run_id, fr.error)
                return {
                    "success": False,
                    "source_id": source_id,
                    "api_id": api_id,
                    "error": fr.error,
                }

            # Convert
            period_label = self._get_period_label(source_id, api_id)
            conv = await convert_to_file(
                data=fr.data,
                source_id=source_id,
                api_config=api_config,
                output_dir=str(self.output_dir),
                content_type=fr.content_type,
                period_label=period_label,
            )
            if not conv.success:
                await self._log_run_error(run_id, conv.error)
                return {
                    "success": False,
                    "source_id": source_id,
                    "api_id": api_id,
                    "error": f"Conversion failed: {conv.error}",
                }

            # Persist period watermark if this was a period-aware fetch
            await self._save_period_watermark(source_id, api_id)

            # Feed pipeline
            feed_result = await feed_file(
                file_path=conv.file_path,
                filename=conv.filename,
                source_config=config,
                api_config=api_config,
                metadata=conv.metadata,
            )

            result = {
                "success": feed_result.get("success", False),
                "source_id": source_id,
                "api_id": api_id,
                "filename": conv.filename,
                "file_id": feed_result.get("file_id"),
                "skipped": feed_result.get("skipped", False),
                "classification": feed_result.get("classification"),
                "routed_to": feed_result.get("routed_to"),
                "sql_job_id": feed_result.get("sql_job_id"),
                "sql_status": feed_result.get("sql_status"),
                "sql_error": feed_result.get("sql_error"),
                "rows_inserted": feed_result.get("rows_inserted"),
                "duration_seconds": (datetime.now() - started_at).total_seconds(),
            }

            await self._log_run_complete(run_id, result)
            return result

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Single API run failed for {source_id}/{api_id}: {error_msg}", exc_info=True)
            await self._log_run_error(run_id, error_msg)
            return {
                "success": False,
                "source_id": source_id,
                "api_id": api_id,
                "error": error_msg,
            }

    # ============ Period Watermark ============

    def _get_period_label(self, source_id: str, api_id: str) -> Optional[str]:
        """Read the pending watermark from the fetcher to use as a filename label."""
        from app.agent.fetcher import _period_watermarks
        return _period_watermarks.get((source_id, api_id))

    async def _save_period_watermark(self, source_id: str, api_id: str):
        """Persist the period watermark recorded by the fetcher, if any."""
        try:
            from app.agent.fetcher import pop_period_watermark
            from app.models.db import update_last_fetched_period

            watermark = pop_period_watermark(source_id, api_id)
            if watermark:
                await update_last_fetched_period(source_id, api_id, watermark)
                logger.info(f"Updated period watermark for {source_id}/{api_id} → {watermark}")
        except Exception as e:
            logger.warning(f"Failed to save period watermark for {source_id}/{api_id}: {e}")

    # ============ Run Logging ============

    async def _log_run_start(self, source_id: str, api_id: Optional[str] = None) -> Optional[int]:
        """Log the start of an agent run to the DB."""
        try:
            from app.models.db import insert_agent_run
            return await insert_agent_run(source_id=source_id, api_id=api_id)
        except Exception as e:
            logger.warning(f"Failed to log run start: {e}")
            return None

    async def _log_run_complete(self, run_id: Optional[int], result: Dict[str, Any]):
        """Log successful completion of an agent run."""
        if run_id is None:
            return
        try:
            from app.models.db import update_agent_run
            await update_agent_run(
                run_id=run_id,
                status="completed",
                files_fetched=result.get("apis_fetched") or (1 if result.get("filename") else 0),
                files_ingested=result.get("files_ingested") or (1 if result.get("file_id") and not result.get("skipped") else 0),
                files_skipped=result.get("files_skipped") or (1 if result.get("skipped") else 0),
                run_details=result,
            )
        except Exception as e:
            logger.warning(f"Failed to log run completion: {e}")

    async def _log_run_error(self, run_id: Optional[int], error_message: Optional[str]):
        """Log a failed agent run."""
        if run_id is None:
            return
        try:
            from app.models.db import update_agent_run
            await update_agent_run(
                run_id=run_id,
                status="failed",
                error_message=error_message,
            )
        except Exception as e:
            logger.warning(f"Failed to log run error: {e}")


# ============ Singleton & Convenience Functions ============

agent_orchestrator = AgentOrchestrator()


async def run_agent_source(source_id: str) -> Dict[str, Any]:
    """Convenience: run all APIs for a specific source."""
    return await agent_orchestrator.run_source(source_id)


async def run_agent_all() -> Dict[str, Any]:
    """Convenience: run all configured sources."""
    return await agent_orchestrator.run_all()


async def run_agent_single_api(source_id: str, api_id: str) -> Dict[str, Any]:
    """Convenience: run a single API endpoint."""
    return await agent_orchestrator.run_single_api(source_id, api_id)
