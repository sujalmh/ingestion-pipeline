"""
Pytest configuration for ingestion-pipeline tests.
E2E tests require orchestrator and SQL Ingestion API to be running.
"""
import os
import pytest

# E2E marker: run with pytest -m e2e (requires both servers up)
def pytest_configure(config):
    config.addinivalue_line("markers", "e2e: end-to-end test with real file uploads (orchestrator + SQL API must be running)")


@pytest.fixture(scope="session")
def orchestrator_url():
    return os.environ.get("ORCHESTRATOR_URL", "http://localhost:8002")


@pytest.fixture(scope="session")
def sql_pipeline_url():
    return os.environ.get("SQL_PIPELINE_API_URL", "http://localhost:8000")
