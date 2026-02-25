"""
End-to-end test: real file upload through the orchestrator API.

Flow:
  1. POST real CSV to orchestrator /inbound/upload-classify-process
  2. Assert upload + classification + SQL pipeline processing
  3. Poll SQL job status until awaiting_approval or schema_mismatch (or completed/failed)
  4. Optionally approve and assert completion

Requires:
  - Orchestrator running (default http://localhost:8002)
  - SQL Ingestion API running (default http://localhost:8000)
  - Orchestrator .env with DATABASE_URL, OPENAI_API_KEY, SQL_PIPELINE_API_URL

Run:
  pytest tests/e2e/test_orchestrator_upload_e2e.py -v -m e2e
  OR with servers on different hosts:
  ORCHESTRATOR_URL=http://localhost:8002 pytest tests/e2e/test_orchestrator_upload_e2e.py -v -m e2e
"""
import os
import time
from pathlib import Path

import httpx
import pytest

# Real sample files: repo/auto-sql-ingestion/tests/sample_data/
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_SAMPLE_DIR = _REPO_ROOT / "auto-sql-ingestion" / "tests" / "sample_data"
_SAMPLE_CSV = _SAMPLE_DIR / "amfi_monthly_sample.csv"
_SAMPLE_JAN2026_CSV = _SAMPLE_DIR / "amfi_monthly_sample_jan2026.csv"
# Fallback if run from ingestion-pipeline only
if not _SAMPLE_CSV.exists():
    _alt = Path(__file__).resolve().parent.parent.parent / ".." / "auto-sql-ingestion" / "tests" / "sample_data"
    _SAMPLE_CSV = _alt / "amfi_monthly_sample.csv"
    _SAMPLE_JAN2026_CSV = _alt / "amfi_monthly_sample_jan2026.csv"
_SAMPLE_CSV = _SAMPLE_CSV.resolve() if _SAMPLE_CSV.exists() else _SAMPLE_CSV
if _SAMPLE_JAN2026_CSV.exists():
    _SAMPLE_JAN2026_CSV = _SAMPLE_JAN2026_CSV.resolve()


def _orchestrator_url():
    return os.environ.get("ORCHESTRATOR_URL", "http://localhost:8002")


def _sql_pipeline_url():
    return os.environ.get("SQL_PIPELINE_API_URL", "http://localhost:8000")


def _check_orchestrator_up(base_url: str) -> bool:
    try:
        r = httpx.get(f"{base_url}/inbound/stats", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def _check_sql_pipeline_up(base_url: str) -> bool:
    try:
        r = httpx.get(f"{base_url}/", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


@pytest.mark.e2e
def test_e2e_real_file_upload_upload_classify_process():
    """Upload a real CSV to orchestrator; full flow: upload -> classify -> process SQL.
    With a clean DB, first load (amfi_monthly_sample.csv) must resolve as sql_otl."""
    base = _orchestrator_url()
    if not _check_orchestrator_up(base):
        pytest.skip("Orchestrator not running at " + base)

    if not _SAMPLE_CSV.exists():
        pytest.skip(f"Sample file not found: {_SAMPLE_CSV}")

    with open(_SAMPLE_CSV, "rb") as f:
        file_content = f.read()

    filename = _SAMPLE_CSV.name

    with httpx.Client(timeout=120) as client:
        # Step 1: Upload + classify + process in one call
        resp = client.post(
            f"{base}/inbound/upload-classify-process",
            files={"file": (filename, file_content, "text/csv")},
            data={"source_type": "direct_upload"},
        )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()

    assert data.get("success") is True, f"Response: {data}"
    assert "upload" in data
    assert data["upload"].get("file_id"), "Missing file_id"
    assert "classification" in data
    classification = data["classification"]
    assert classification.get("success") is True, classification
    routed_to = classification.get("routed_to")
    assert routed_to == "sql_otl", (
        f"First load (clean state) should route to sql_otl, got {routed_to}"
    )

    assert "processing" in data
    processing = data["processing"]
    assert processing.get("pipeline") == "sql"
    assert processing.get("success") is True, processing
    sql_job_id = processing.get("sql_job_id")
    assert sql_job_id, "Missing sql_job_id after SQL pipeline processing"

    file_id = data["upload"]["file_id"]

    # Step 2: Poll SQL job status until terminal or approvable (via SQL API)
    sql_base = _sql_pipeline_url()
    if not _check_sql_pipeline_up(sql_base):
        pytest.skip("SQL Ingestion API not running at " + sql_base)

    terminal_or_approvable = ("awaiting_approval", "schema_mismatch", "completed", "incremental_load_completed", "failed")
    status = processing.get("sql_status") or "preprocessing"
    for _ in range(60):  # up to ~2 min
        if status in terminal_or_approvable:
            break
        time.sleep(2)
        with httpx.Client(timeout=15) as c:
            r = c.get(f"{sql_base}/status/{sql_job_id}")
        if r.status_code != 200:
            break
        status = r.json().get("status", status)

    assert status in terminal_or_approvable, f"SQL job did not reach terminal/approvable state: {status}"


@pytest.mark.e2e
def test_e2e_approve_flow_after_upload():
    """
    Full E2E: upload real file -> wait for awaiting_approval/schema_mismatch -> approve via orchestrator -> assert success.
    """
    base = _orchestrator_url()
    sql_base = _sql_pipeline_url()
    if not _check_orchestrator_up(base) or not _check_sql_pipeline_up(sql_base):
        pytest.skip("Orchestrator or SQL API not running")

    if not _SAMPLE_CSV.exists():
        pytest.skip(f"Sample file not found: {_SAMPLE_CSV}")

    with open(_SAMPLE_CSV, "rb") as f:
        file_content = f.read()
    filename = _SAMPLE_CSV.name

    with httpx.Client(timeout=120) as client:
        resp = client.post(
            f"{base}/inbound/upload-classify-process",
            files={"file": (filename, file_content, "text/csv")},
            data={"source_type": "direct_upload"},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("success") is True
    file_id = data["upload"]["file_id"]
    sql_job_id = data.get("processing", {}).get("sql_job_id")
    assert sql_job_id

    # Wait until job is approvable
    status = "preprocessing"
    for _ in range(90):
        if status in ("awaiting_approval", "schema_mismatch"):
            break
        if status in ("completed", "incremental_load_completed", "failed"):
            pytest.skip(f"Job ended without approval step: {status}")
        time.sleep(2)
        with httpx.Client(timeout=15) as c:
            r = c.get(f"{sql_base}/status/{sql_job_id}")
        if r.status_code != 200:
            break
        status = r.json().get("status", status)

    if status not in ("awaiting_approval", "schema_mismatch"):
        pytest.skip(f"Job not in approvable state after wait: {status}")

    # Approve via orchestrator proxy
    with httpx.Client(timeout=90) as client:
        approve_resp = client.post(
            f"{base}/approve/{file_id}/approve-sql",
            data={
                "source": "E2E Test Source",
                "source_url": "https://example.com/e2e",
                "released_on": "2026-02-20T00:00:00",
                "updated_on": "2026-02-20T00:00:00",
                "business_metadata": "E2E test run",
            },
        )

    assert approve_resp.status_code == 200, f"Approve failed: {approve_resp.status_code} {approve_resp.text}"
    approve_data = approve_resp.json()
    assert approve_data.get("success") is True or approve_data.get("sql_job_status") == "incremental_load_completed", (
        f"Approval did not succeed: {approve_data}"
    )


@pytest.mark.e2e
def test_e2e_second_load_resolves_inc():
    """
    Upload amfi_monthly_sample_jan2026.csv; must resolve as sql_inc when a completed
    load for amfi_monthly_sample already exists (run after first load approved/completed).
    """
    base = _orchestrator_url()
    if not _check_orchestrator_up(base):
        pytest.skip("Orchestrator not running at " + base)
    if not _SAMPLE_JAN2026_CSV.exists():
        pytest.skip(f"Sample file not found: {_SAMPLE_JAN2026_CSV}")

    with open(_SAMPLE_JAN2026_CSV, "rb") as f:
        file_content = f.read()
    filename = _SAMPLE_JAN2026_CSV.name

    with httpx.Client(timeout=120) as client:
        resp = client.post(
            f"{base}/inbound/upload-classify-process",
            files={"file": (filename, file_content, "text/csv")},
            data={"source_type": "direct_upload"},
        )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data.get("success") is True, f"Response: {data}"
    classification = data.get("classification", {})
    assert classification.get("success") is True, classification
    routed_to = classification.get("routed_to")
    assert routed_to == "sql_inc", (
        f"Second load (after first completed) should route to sql_inc, got {routed_to}"
    )
