# Data API Ingestion Agent — Documentation

## Overview

The **Data API Ingestion Agent** automatically fetches data from external APIs, converts it into files, and feeds them into the ingestion pipeline for classification and storage. It supports two types of data sources:

| Source Type | How It's Configured | Editable via API? | Example |
|------------|-------------------|------------------|---------|
| **YAML Sources** | Pre-configured `.yaml` files in `app/agent/sources/` | ❌ Read-only | Government APIs (MoSPI) |
| **DB Sources** | Registered at runtime via REST API or management UI | ✅ Full CRUD | Business partner APIs |

---

## Quick Start

### 1. Access the Management UI

Open your browser and navigate to:

```
http://<server-ip>:8073/agent/manage
```

This dashboard lets you:
- View all configured sources (YAML + DB)
- Register new API sources
- Test-fetch APIs before saving
- Run sources manually
- Enable/disable sources and endpoints

### 2. Test an API

Before registering a source, test that the API works:

**Via UI:**
1. Click **🧪 Test an API**
2. Enter the base URL, endpoint, method, and auth details
3. Click **Test Fetch**
4. Review the preview (row count, sample data)

**Via cURL:**
```bash
curl -X POST http://<server>:8073/agent/sources/test \
  -H "Content-Type: application/json" \
  -d '{
    "base_url": "https://api.example.com",
    "endpoint": "/data",
    "method": "GET",
    "auth_type": "none"
  }'
```

### 3. Register a New Source

**Via UI:**
1. Click **+ Add New Source**
2. Fill in source details (ID, name, base URL, auth)
3. Add one or more API endpoints
4. Click **Register Source**

**Via cURL:**
```bash
curl -X POST http://<server>:8073/agent/sources \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "my_company_api",
    "name": "My Company Data Feed",
    "base_url": "https://api.mycompany.com",
    "auth_type": "api_key",
    "auth_credentials": {
      "api_key": "your-api-key-here"
    },
    "apis": [
      {
        "api_id": "sales_data",
        "name": "Daily Sales Report",
        "endpoint": "/v2/reports/sales",
        "method": "GET",
        "output_format": "json",
        "schedule": "daily",
        "params": {"format": "detailed"},
        "metadata": {
          "major_domain": "Business",
          "sub_domain": "Sales"
        }
      }
    ]
  }'
```

### 4. Run the Agent

**Run all sources:**
```bash
curl -X POST http://<server>:8073/agent/run
```

**Run a specific source:**
```bash
curl -X POST http://<server>:8073/agent/run/my_company_api
```

**Run a single API endpoint:**
```bash
curl -X POST http://<server>:8073/agent/run/my_company_api/sales_data
```

### 5. Check Run History

```bash
curl http://<server>:8073/agent/status
```

Filter by source:
```bash
curl "http://<server>:8073/agent/status?source_id=mospi&limit=10"
```

---

## API Reference

### Agent Execution

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/agent/run` | Run all enabled sources |
| `POST` | `/agent/run/{source_id}` | Run all APIs for a source |
| `POST` | `/agent/run/{source_id}/{api_id}` | Run a single API |
| `GET` | `/agent/status` | Get run history (optional: `?source_id=X&limit=N`) |

### Source Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/agent/sources` | List all sources (YAML + DB merged) |
| `GET` | `/agent/sources/{source_id}` | Get source details |
| `POST` | `/agent/sources` | Register a new DB source |
| `PUT` | `/agent/sources/{source_id}` | Update a DB source |
| `DELETE` | `/agent/sources/{source_id}` | Delete a DB source (cascades) |
| `PATCH` | `/agent/sources/{source_id}/toggle?enabled=true` | Enable/disable source |

### Endpoint Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/agent/sources/{source_id}/apis` | Add an API endpoint |
| `PUT` | `/agent/sources/{source_id}/apis/{api_id}` | Update an endpoint |
| `DELETE` | `/agent/sources/{source_id}/apis/{api_id}` | Delete an endpoint |
| `PATCH` | `/agent/sources/{source_id}/apis/{api_id}/toggle?enabled=true` | Toggle endpoint |

### Utility

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/agent/sources/test` | Test-fetch an API without saving |
| `GET` | `/agent/manage` | Management UI (HTML page) |

---

## Authentication Types

The agent supports three authentication modes:

### `none` — No Authentication
```json
{
  "auth_type": "none"
}
```
Used for public APIs (e.g., MoSPI data endpoints).

### `token` — Login-based Bearer Token
```json
{
  "auth_type": "token",
  "auth_credentials": {
    "email": "user@example.com",
    "password": "secret"
  }
}
```
The agent will `POST` to the source's `auth_login_url` with the credentials, extract the bearer token from the response, and attach it as `Authorization: Bearer <token>` to all data requests. Tokens are cached and auto-refreshed before expiry.

For YAML sources, credentials reference environment variables:
```yaml
auth:
  type: token
  login_url: "/api/login"
  token_expiry_minutes: 14
  credentials_env:
    email: MY_API_EMAIL      # reads from $MY_API_EMAIL
    password: MY_API_PASSWORD # reads from $MY_API_PASSWORD
```

### `api_key` — API Key Header
```json
{
  "auth_type": "api_key",
  "auth_credentials": {
    "api_key": "your-key-here"
  }
}
```
Attaches `X-API-Key: <key>` header (or reads from env var for YAML sources).

---

## Adding YAML Sources (Developer)

To add a pre-configured source, create a `.yaml` file in `app/agent/sources/`:

```yaml
# app/agent/sources/rbi.yaml
source_id: rbi
name: "Reserve Bank of India"
base_url: "https://api.rbi.org.in"
auth:
  type: api_key
  credentials_env:
    api_key: RBI_API_KEY

apis:
  - id: exchange_rates
    name: "Daily Exchange Rates"
    endpoint: "/api/rates/exchange"
    method: GET
    description: "USD/INR and other currency pairs"
    output_format: json
    params:
      currency: "USD"
    schedule: daily
    metadata:
      major_domain: "Economy"
      sub_domain: "Forex"
      source: "RBI"

  - id: repo_rate
    name: "Repo Rate History"
    endpoint: "/api/rates/repo"
    method: GET
    output_format: json
    schedule: monthly
    metadata:
      major_domain: "Economy"
      sub_domain: "Monetary Policy"
      source: "RBI"
```

YAML sources are loaded at startup and cannot be modified via the API.

---

## Source ID Naming Convention

Source IDs must be lowercase alphanumeric with underscores:
- ✅ `mospi`, `rbi_data`, `company_sales_v2`
- ❌ `MoSPI`, `rbi-data`, `my source`

API IDs follow the same convention:
- ✅ `cpi_index`, `get_rates`, `daily_report`
- ❌ `CPI-Index`, `get rates`

---

## What Happens When You Run the Agent

```
1. Source Resolution
   ├── Load YAML configs from app/agent/sources/*.yaml
   └── Load DB sources from source_registry table
   
2. Authentication (if needed)
   ├── Check for cached token
   ├── If expired → POST to login_url with credentials
   └── Cache new token with expiry timestamp

3. Fetch Data
   ├── Build full URL: base_url + endpoint + params
   ├── Attach auth headers (Bearer token / API key)
   ├── HTTP request with retry (3 attempts, exponential backoff)
   └── Return response body

4. Convert to File
   ├── JSON array → .json file
   ├── Nested JSON (with "data" key) → extract array → .json file
   ├── CSV text → .csv file
   └── Filename: {source_id}_{api_id}_{YYYYMMDD}.{ext}

5. Feed into Pipeline
   ├── Check for duplicate (same filename already registered)
   ├── Register file via register_uploaded_file()
   ├── Classify via classify_file_by_id()
   └── Route to SQL or Vector pipeline

6. Log Results
   └── Insert/update agent_run_log table
```

---

## Database Tables

### `agent_run_log`
Tracks every agent execution.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `source_id` | VARCHAR(100) | Which source was run |
| `api_id` | VARCHAR(100) | Which API (null = all) |
| `status` | VARCHAR(50) | `running`, `completed`, `failed` |
| `started_at` | TIMESTAMP | When the run started |
| `completed_at` | TIMESTAMP | When it finished |
| `files_fetched` | INT | APIs that returned data |
| `files_ingested` | INT | Files successfully ingested |
| `files_skipped` | INT | Duplicates skipped |
| `error_message` | TEXT | Error details if failed |
| `run_details` | JSONB | Full result payload |

### `source_registry`
Stores runtime-registered API sources.

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | VARCHAR(100) | Unique identifier |
| `name` | VARCHAR(500) | Human-readable name |
| `base_url` | VARCHAR(2000) | API base URL |
| `auth_type` | VARCHAR(20) | `none`, `token`, `api_key` |
| `auth_credentials` | JSONB | Credentials (email/password, api_key) |
| `enabled` | BOOLEAN | Whether this source is active |
| `created_at` | TIMESTAMP | Registration time |

### `source_api_endpoints`
Individual API endpoints within a source.

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | VARCHAR(100) | FK to source_registry |
| `api_id` | VARCHAR(100) | Unique within source |
| `endpoint` | VARCHAR(2000) | URL path (e.g., `/api/data`) |
| `method` | VARCHAR(10) | `GET` or `POST` |
| `output_format` | VARCHAR(10) | `json` or `csv` |
| `params` | JSONB | Query parameters |
| `schedule` | VARCHAR(20) | `manual`, `daily`, `weekly`, `monthly` |

---

## Architecture

```
app/agent/
├── __init__.py           # Module exports
├── source_config.py      # Pydantic models + YAML/DB loader + unified resolution
├── auth_manager.py       # Token/API key management with caching
├── fetcher.py            # HTTP fetcher with retry + test-fetch
├── converter.py          # JSON/CSV → file conversion
├── pipeline_feeder.py    # Register + classify files into pipeline
├── orchestrator.py       # Main coordinator (run all / run source / run single)
└── sources/
    └── mospi.yaml        # Pre-configured MoSPI source
```

### Module Responsibilities

| Module | Role |
|--------|------|
| **source_config.py** | Single source of truth — merges YAML + DB sources into unified `SourceConfig` objects |
| **auth_manager.py** | Handles login flows, caches tokens, auto-refreshes before expiry, resolves env vars |
| **fetcher.py** | Makes HTTP requests with configurable timeout, retry (exponential backoff), and error handling |
| **converter.py** | Converts API responses to files — handles JSON arrays, nested JSON objects, and CSV text |
| **pipeline_feeder.py** | Bridge to existing pipeline — calls `register_uploaded_file()` then `classify_file_by_id()` |
| **orchestrator.py** | Top-level coordinator — runs sources/APIs, aggregates results, logs to DB |

### Key Design Decisions

1. **YAML sources take priority** — if the same `source_id` exists in both YAML and DB, the YAML version wins
2. **YAML sources are immutable via API** — you cannot edit, delete, or toggle YAML sources through the REST API (returns 403)
3. **Lazy imports** — agent modules are imported at runtime in API endpoints to avoid circular imports
4. **Duplicate detection** — files with the same name (e.g., `mospi_cpi_index_20260305.json`) are detected and skipped
5. **No pandas dependency** — the converter uses only the stdlib `csv` module

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MOSPI_EMAIL` | — | MoSPI login email |
| `MOSPI_PASSWORD` | — | MoSPI login password |
| `DATA_GOV_API_KEY` | — | data.gov.in API key (future) |
| `AGENT_SCHEDULE_ENABLED` | `false` | Enable scheduled runs |
| `AGENT_SCHEDULE_INTERVAL_HOURS` | `24` | Hours between scheduled runs |
| `AGENT_FETCH_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `AGENT_RETRY_ATTEMPTS` | `3` | Max retry attempts per request |

---

## Troubleshooting

### Source registered but not showing up
Check the server log for parsing errors:
```bash
tail -50 server.log | grep "Failed to parse"
```

### API returns 404
- Verify the endpoint path is correct (try in browser or curl first)
- Check if the API requires POST instead of GET

### Authentication failures
- For YAML sources: check that env vars are set (e.g., `echo $MOSPI_EMAIL`)
- For DB sources: verify credentials in the source registration payload
- Check if the login URL is correct

### Duplicate files skipped
This is by design — the agent detects files with the same name and skips re-ingestion. To re-ingest, delete the existing file first via the dashboard.

### Check run history
```bash
curl http://<server>:8073/agent/status?limit=5
```
