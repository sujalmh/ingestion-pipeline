# E2E Tests (Real File Uploads)

End-to-end tests hit the **orchestrator API** with **real files** (no mocks). They require both services to be running.

## Prerequisites

1. **SQL Ingestion API (auto-sql-ingestion)** running, e.g.:
   ```bash
   cd auto-sql-ingestion && python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```
2. **Orchestrator** running, with `.env` pointing to the SQL API:
   ```bash
   cd ingestion-pipeline && python -m uvicorn app.main:app --host 0.0.0.0 --port 8002
   ```
   In `.env`: `SQL_PIPELINE_API_URL=http://localhost:8000`, plus `DATABASE_URL`, `OPENAI_API_KEY`, etc.

3. **Sample file** used by tests: `auto-sql-ingestion/tests/sample_data/amfi_monthly_sample.csv` (must exist).

## Run E2E tests

From **ingestion-pipeline** (or repo root with correct PYTHONPATH):

```bash
cd ingestion-pipeline
pytest tests/e2e/test_orchestrator_upload_e2e.py -v -m e2e
```

If servers are on different hosts/ports:

```bash
ORCHESTRATOR_URL=http://localhost:8002 SQL_PIPELINE_API_URL=http://localhost:8000 pytest tests/e2e/test_orchestrator_upload_e2e.py -v -m e2e
```

- If the orchestrator or SQL API is not reachable, E2E tests are **skipped** (not failed).
- **test_e2e_real_file_upload_upload_classify_process**: uploads a real CSV, asserts classification + SQL pipeline run, polls until job is in an approvable or terminal state.
- **test_e2e_approve_flow_after_upload**: same upload, then approves via the orchestrator and asserts success.

## Run all tests except E2E

```bash
pytest -v -m "not e2e"
```
