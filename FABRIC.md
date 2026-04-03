# Fabric Deployment Guide — Predictive Maintenance from Connected Vehicle Telemetry

End-to-end Medallion Architecture (Bronze → Silver → Gold) on Microsoft Fabric with an import-mode semantic model, Power BI report, and orchestration pipeline.

---

## Quick Start

```powershell
# 1. Login to Azure
az login --tenant <YOUR_TENANT_ID>
az account set --subscription <YOUR_SUBSCRIPTION_ID>

# 2. Deploy infrastructure + notebooks via script
.\scripts\deploy.ps1 -WorkspaceName "automotive-predictive-maint-dev" -CapacityId "<YOUR_CAPACITY_ID>"

# 3. Complete setup manually in the Fabric portal (see detailed steps below):
#    a. Attach lakehouses to notebooks
#    b. Run notebooks in order: Bronze → Silver → Gold
#    c. Configure semantic model credentials for the SQL endpoint connections
#    d. Create or reconnect the Power BI Report to the semantic model
```

---

## Architecture

```
FABRIC WORKSPACE
────────────────────────────────────────────────────────────

   [ Bronze ] ───▶ [ Silver ] ───▶ [ Gold ]

   Bronze Lakehouse        Silver Lakehouse        Gold Lakehouse
   ─────────────────       ─────────────────       ─────────────────
   raw_vehicles            dim_vehicles            agg_vehicle_health_score
   raw_telemetry           fact_telemetry          agg_fleet_daily_summary
   raw_dtc_events          fact_dtc_events         agg_maintenance_cost_analysis
   raw_maintenance         fact_maintenance        agg_dtc_frequency

   📁 Notebook/  (workspace folder)
          │                          │                        │
   ┌────────────────┐      ┌───────────────────┐        ┌─────────────────┐
   │01_bronze_ingest│      │02_silver_transform│        │03_gold_aggregate│
   └────────────────┘      └───────────────────┘        └─────────────────┘
                                     │
                              ┌──────────────┐
                              │ Import Mode  │
                              │ Semantic Mdl │  ← Defined in fabric/semantic-model
                              └──────┬───────┘
                                     │
                               ┌──────────────┐
                               │  Power BI    │  ← Created or rebound manually in portal
                               │   Report     │
                               └──────────────┘

   Medallion ETL Pipeline:
   Bronze NB ───▶ Silver NB ───▶ Gold NB
```

---

## Part A — Scripted Deployment (API)

Steps 1–4 and 7 are automated by `scripts/deploy.ps1`.

### Step 1 — Create Workspace

```powershell
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces" `
  --body '{"displayName":"automotive-predictive-maint-dev"}'
```

### Step 2 — Assign Capacity

Requires an existing F2+ capacity.

```powershell
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/assignToCapacity" `
  --body '{"capacityId":"<CAPACITY_ID>"}'
```

### Step 3 — Create Lakehouses

Creates `automotive_bronze`, `automotive_silver`, `automotive_gold`. Each auto-generates a SQL Endpoint.

```powershell
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" `
  --body '{"displayName":"automotive_bronze","type":"Lakehouse"}'
```

### Step 4 — Upload Notebooks

Notebooks uploaded via `POST /v1/workspaces/{id}/items` with `definition.format = "ipynb"`. The deploy script replaces `{{WORKSPACE_ID}}` / `{{LAKEHOUSE_ID}}` placeholders with actual IDs.

All three notebooks are placed inside the **`Notebook`** workspace folder, which is created automatically by the deploy script if it does not exist.

```powershell
# Create the Notebook folder
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/folders" `
  --body '{"displayName":"Notebook"}'

# Move a notebook into the folder after creation
az rest --method patch --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items/$NB_ID" `
  --body "{\"folderId\":\"$FOLDER_ID\"}"
```

Notebooks in `notebooks/`:
- **01_bronze_ingest.ipynb** — generates 100 vehicles, 5K telemetry, 3K DTCs, 2K maintenance records
- **02_silver_transform.ipynb** — dedup, null handling, anomaly flags, quality scores
- **03_gold_aggregate.ipynb** — composite health scores, fleet daily KPIs, cost analysis, DTC frequency

---

## Semantic Model Design

The repository semantic model now uses an import-mode, multi-source design:

- Silver-backed dimension and transactional fact tables:
  - `dim_vehicle` from `automotive_silver.dbo.dim_vehicles`
  - `fact_telemetry_readings` from `automotive_silver.dbo.fact_telemetry`
  - `fact_dtc_events` from `automotive_silver.dbo.fact_dtc_events`
  - `fact_maintenance_events` from `automotive_silver.dbo.fact_maintenance`
- Gold-backed aggregate fact tables:
  - `fact_vehicle_health_score` from `automotive_gold.dbo.agg_vehicle_health_score`
  - `fact_fleet_daily_summary` from `automotive_gold.dbo.agg_fleet_daily_summary`
  - `fact_maintenance_cost_analysis` from `automotive_gold.dbo.agg_maintenance_cost_analysis`
  - `fact_dtc_frequency` from `automotive_gold.dbo.agg_dtc_frequency`
- Relationships are kept only where the key is stable and granular enough:
  - `fact_telemetry_readings` → `dim_vehicle`
  - `fact_dtc_events` → `dim_vehicle`
  - `fact_maintenance_events` → `dim_vehicle`
  - `fact_vehicle_health_score` → `dim_vehicle`
- Gold aggregate tables that are grouped by date, fleet, or category remain disconnected by design.

This structure reflects the decisions made during debugging:

- Direct Lake via REST was abandoned for this sample because the service-created bindings were unreliable.
- The semantic model uses import mode over the Fabric SQL endpoint instead.
- The model intentionally mixes Silver and Gold sources so detailed analysis and business aggregates coexist in one dataset.
- Table names use lowercase `dim_` and `fact_` prefixes to make their role explicit.

## Part B — Manual Portal Steps (REQUIRED)

> **Why manual?** Notebook lakehouse attachment still requires the Fabric portal UI. For the import-mode semantic model, the dataset can be deployed from TMDL, but the Power BI service still needs explicit cloud connection credentials for the SQL endpoint data sources before refreshes succeed. Report creation or rebinding is still easiest in the portal.

### Step 5 — Attach Lakehouses to Notebooks

After `deploy.ps1` creates notebooks, each one must be manually connected to its lakehouse.

1. Open **https://app.fabric.microsoft.com** → navigate to your workspace
2. Open the **`Notebook`** folder in the workspace item list
3. For each notebook:

| Notebook | Lakehouse to Attach |
|----------|-------------------|
| `01_bronze_ingest` | `automotive_bronze` |
| `02_silver_transform` | `automotive_silver` |
| `03_gold_aggregate` | `automotive_gold` |

4. Open the notebook → click **"Add Lakehouse"** in the left panel → **"Existing lakehouse"** → select the lakehouse → click **"Add"**
5. The lakehouse should appear in the Explorer panel with `Tables/` and `Files/` folders

### Step 6 — Run Notebooks in Order

Run the notebooks sequentially. Each one depends on the output of the previous.

1. Open `01_bronze_ingest` → click **"Run all"** → wait for completion (~2 min)
   - Verify: 5 tables appear under `automotive_bronze` → Tables: `raw_vehicles`, `raw_telemetry`, `raw_dtc_events`, `raw_maintenance`, `raw_service_centers`
2. Open `02_silver_transform` → click **"Run all"** → wait for completion (~3 min)
   - Verify: 4 tables under `automotive_silver` → Tables: `dim_vehicles`, `fact_telemetry`, `fact_dtc_events`, `fact_maintenance`
3. Open `03_gold_aggregate` → click **"Run all"** → wait for completion (~2 min)
   - Verify: 4 tables under `automotive_gold` → Tables: `agg_vehicle_health_score`, `agg_fleet_daily_summary`, `agg_maintenance_cost_analysis`, `agg_dtc_frequency`

### Step 7 — Create Data Pipeline

The deployment script creates this pipeline automatically. It orchestrates Bronze → Silver → Gold sequentially using `TridentNotebook` activities with `dependsOn` chains.

```powershell
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/items" `
  --body "@$env:TEMP\pipeline.json"
```

If you prefer orchestration over manual notebook execution, use the Medallion ETL Pipeline to run the three notebooks in sequence before continuing to Step 8.

### Step 8 — Configure the Semantic Model (Import Mode)

The semantic model definition lives in `fabric/semantic-model/` and is intended to be published as TMDL. Before this step, make sure the Silver and Gold tables exist by completing Step 6 manually or by running the pipeline from Step 7. After deployment, configure the Power BI service connections in the portal.

1. Navigate to the workspace in the Fabric portal.
2. Open the semantic model **`Automotive Predictive Maintenance`**.
3. In **Settings** → **Data source credentials**, configure the cloud connections for both import sources:

| Connection | Database |
|------------|----------|
| Fabric SQL endpoint server | `automotive_silver` |
| Fabric SQL endpoint server | `automotive_gold` |

4. Use organizational credentials that can read both SQL endpoint databases.
5. Trigger a refresh and verify that all eight tables load successfully.
6. In model view, confirm the relationship set remains limited to the vehicle-grain tables and that the disconnected Gold aggregates stay disconnected.

#### Included Semantic Model Tables

| Semantic Table | Source |
|----------------|--------|
| `dim_vehicle` | `automotive_silver.dbo.dim_vehicles` |
| `fact_telemetry_readings` | `automotive_silver.dbo.fact_telemetry` |
| `fact_dtc_events` | `automotive_silver.dbo.fact_dtc_events` |
| `fact_maintenance_events` | `automotive_silver.dbo.fact_maintenance` |
| `fact_vehicle_health_score` | `automotive_gold.dbo.agg_vehicle_health_score` |
| `fact_fleet_daily_summary` | `automotive_gold.dbo.agg_fleet_daily_summary` |
| `fact_maintenance_cost_analysis` | `automotive_gold.dbo.agg_maintenance_cost_analysis` |
| `fact_dtc_frequency` | `automotive_gold.dbo.agg_dtc_frequency` |

#### Key Measures Already Defined in TMDL

- `fact_vehicle_health_score`: `Avg Health Score`, `High Risk %`, `Avg Vehicle Maintenance Cost`
- `fact_fleet_daily_summary`: `Total Anomaly Events`, `Avg Anomaly Rate %`, `Total Active Vehicles`
- `fact_maintenance_cost_analysis`: `Total Maintenance Cost Amount`, `Avg Cost Per Service`, `Total Services`, `Warranty Claim %`, `Total Warranty Cost`
- `fact_dtc_frequency`: `Total DTC Occurrences`, `Total Affected Vehicles`, `Avg Active Rate %`
- Silver facts keep the detailed operational measures already defined in the repo TMDL.

### Step 9 — Create Power BI Report in Portal

Create a report connected to the semantic model.

1. From the workspace, click the semantic model **`Automotive Predictive Maintenance`**
2. Click **"Create report"** → **"Start from scratch"** (or **"Auto-create a report"** for a quick start)
3. Build 3 report pages:

#### Page 1 — Fleet Overview

| Visual | Type | Fields |
|--------|------|--------|
| Total Vehicles | Card | `[Total Vehicles]` |
| Avg Health Score | Card | `[Avg Health Score]` |
| High Risk Count | Card | `[High Risk Vehicles]` |
| Total Cost | Card | `[Total Cost]` |
| Health by Make | Bar chart | Axis: `Make`, Value: `[Avg Health Score]` |
| Risk Distribution | Donut chart | Legend: `Risk Level`, Value: count of `VIN` |
| Vehicle Details | Table | `VIN`, `Make`, `Model`, `Health Score`, `Risk Level` |

#### Page 2 — Maintenance Analysis

| Visual | Type | Fields |
|--------|------|--------|
| Cost by Type | Bar chart | Axis: `Maintenance Type`, Value: `[Total Cost]` |
| Cost by Make | Bar chart | Axis: `Make`, Value: `[Total Cost]` |
| Maintenance Details | Table | `Make`, `Maintenance Type`, `Category`, `Service Count`, `Total Cost`, `Avg Cost` |

#### Page 3 — DTC Analysis

| Visual | Type | Fields |
|--------|------|--------|
| DTC by System | Bar chart | Axis: `System Category`, Value: `[Total DTC Occurrences]` |
| Severity Breakdown | Donut chart | Legend: `Severity`, Value: `[Total DTC Occurrences]` |
| DTC Details | Table | `DTC Code`, `DTC Description`, `System Category`, `Severity`, `Occurrence Count`, `Affected Vehicles` |

4. Save the report as **"Automotive Predictive Maintenance"**

---

## Repository Structure

```
├── notebooks/
│   ├── 01_bronze_ingest.ipynb         # PySpark: synthetic data → Bronze lakehouse
│   ├── 02_silver_transform.ipynb      # PySpark: clean & validate → Silver
│   └── 03_gold_aggregate.ipynb        # PySpark: business aggregates → Gold
├── fabric/
│   └── pipeline/
│       └── pipeline-content.json      # Bronze → Silver → Gold orchestration
├── scripts/
│   ├── deploy.ps1                     # Deploys workspace, lakehouses, notebooks, pipeline
│   └── update-semantic-model.ps1      # Updates the semantic model TMDL definition
└── FABRIC.md
```

## Remote Workspace Folder Layout

```
automotive-predictive-maint-dev (workspace)
├── 📁 Notebook/
│   ├── 01_bronze_ingest          (Notebook)
│   ├── 02_silver_transform       (Notebook)
│   └── 03_gold_aggregate         (Notebook)
├── automotive_bronze             (Lakehouse)
├── automotive_silver             (Lakehouse)
├── automotive_gold               (Lakehouse)
├── Automotive Predictive Maintenance  (SemanticModel)
├── Automotive Predictive Maintenance  (Report)
└── Medallion ETL Pipeline             (DataPipeline)
```

---

## API Reference

| Task | HTTP Method | Endpoint | Resource |
|------|-------------|----------|----------|
| List workspaces | GET | `/v1/workspaces` | Fabric API |
| Create workspace | POST | `/v1/workspaces` | Fabric API |
| Assign capacity | POST | `/v1/workspaces/{id}/assignToCapacity` | Fabric API |
| Create item (Lakehouse, Notebook, Pipeline) | POST | `/v1/workspaces/{id}/items` | Fabric API |
| List items by type | GET | `/v1/workspaces/{id}/items?type={Type}` | Fabric API |
| Delete item | DELETE | `/v1/workspaces/{id}/{type}/{id}` | Fabric API |
| Poll LRO status | GET | `/v1/operations/{operationId}` | Fabric API |
| Get LRO result | GET | `/v1/operations/{operationId}/result` | Fabric API |
| Run notebook job | POST | `/v1/workspaces/{id}/items/{id}/jobs/instances?jobType=RunNotebook` | Fabric API |
| Sync SQL endpoint metadata | POST | `/v1/workspaces/{id}/sqlEndpoints/{id}/refreshMetadata` | Fabric API |

All API calls use:
```powershell
az rest --method <METHOD> --resource "https://api.fabric.microsoft.com" --url "<FULL_URL>"
```

---

## Placeholder Tokens

The deploy script (`scripts/deploy.ps1`) resolves `{{TOKEN}}` placeholders dynamically.

| Token | Used In | Resolved From |
|-------|---------|---------------|
| `{{WORKSPACE_ID}}` | notebooks, pipeline JSON | Created/found in Step 1 |
| `{{BRONZE_LAKEHOUSE_ID}}` | 02_silver_transform.ipynb | Created in Step 3 |
| `{{SILVER_LAKEHOUSE_ID}}` | 03_gold_aggregate.ipynb | Created in Step 3 |
| `{{GOLD_LAKEHOUSE_ID}}` | 03_gold_aggregate.ipynb | Created in Step 3 |
| `{{BRONZE_NOTEBOOK_ID}}` | pipeline-content.json | Created in Step 4 |
| `{{SILVER_NOTEBOOK_ID}}` | pipeline-content.json | Created in Step 4 |
| `{{GOLD_NOTEBOOK_ID}}` | pipeline-content.json | Created in Step 4 |

---

## Data Model

### Bronze (Raw)
| Table | Rows | Description |
|-------|------|-------------|
| `raw_vehicles` | 100 | Vehicle fleet master data |
| `raw_telemetry` | 5,000 | Sensor readings (engine temp, oil pressure, etc.) |
| `raw_dtc_events` | 3,000 | Diagnostic Trouble Codes |
| `raw_maintenance` | 2,000 | Service and repair records |

### Silver (Cleaned)
| Table | Rows | Transformations |
|-------|------|-----------------|
| `dim_vehicles` | 100 | Deduplicated by VIN, trimmed, mileage validated |
| `fact_telemetry` | 5,000 | Deduped, range validation, anomaly flags, quality score |
| `fact_dtc_events` | 3,000 | Deduped, severity_rank added (1-4) |
| `fact_maintenance` | 2,000 | Deduped, cost validated, service date parsed |

### Gold (Aggregated)
| Table | Columns | Purpose |
|-------|---------|---------|
| `agg_vehicle_health_score` | vin, make, model, year, mileage, region, fleet_id, total_readings, anomaly_rate, total_dtcs, total_severity_points, unique_dtc_codes, critical_dtc_count, total_services, total_maintenance_cost, days_since_last_service, sensor_score, dtc_score, maintenance_score, health_score, risk_level | Composite 0-100 health score per vehicle |
| `agg_fleet_daily_summary` | reading_date, fleet_id, region, active_vehicles, total_readings, avg_engine_temp, avg_oil_pressure, avg_battery_voltage, avg_speed, engine_anomaly_count, oil_anomaly_count, battery_anomaly_count, total_anomalies, anomaly_rate_pct | Daily fleet KPIs by region and fleet ID |
| `agg_maintenance_cost_analysis` | fleet_id, region, make, model, maintenance_type, category, service_count, total_cost, avg_cost, total_labor_hours, avg_labor_hours, total_parts_replaced, warranty_claims, warranty_cost, warranty_rate_pct | Cost breakdown by make, type, category |
| `agg_dtc_frequency` | dtc_code, dtc_description, system_category, severity, event_date, fleet_id, region, make, occurrence_count, affected_vehicles, active_count, active_rate_pct | DTC code frequency by system, severity, region |

---

## Lessons Learned

### 1. Lakehouse Must Be Attached in Fabric Portal (CRITICAL)

**Impact**: Notebooks created via REST API fail with `"Job instance failed without detail error"`.

**Root cause**: `defaultLakehouse` in notebook metadata is not enough — a notebook cannot access the lakehouse data path (`Tables/`) until the lakehouse is **manually attached through the Fabric portal UI**.

**Fix**: Open each notebook in the portal → "Add Lakehouse" → select the corresponding lakehouse.

### 2. Import-Mode Semantic Models Need Explicit SQL Credentials (CRITICAL)

**Impact**: Import-mode semantic models created from TMDL deploy successfully, but refresh still fails until the Power BI service has valid cloud connection credentials for every SQL endpoint datasource.

**Root cause**: The model definition can be published through Fabric REST, but connection credentials are stored separately in the Power BI service and are not fully established by the TMDL upload itself.

**Fix**: Keep the semantic model in import mode, publish it from `fabric/semantic-model`, then configure credentials for both `automotive_silver` and `automotive_gold` in the portal before refreshing.

### 3. Notebook ipynb `source` Must Be String Array

**Error**: `InvalidNotebookContent` — "Error converting value ... to type 'System.Collections.Generic.List<String>'"

**Fix**: Each cell's `source` field must be an array of strings (one per line ending with `\n`):
```json
"source": ["import os\n", "print('hello')"]
```

### 4. `updateDefinition` Breaks Notebook Execution State

**Fix**: Prefer creating notebooks fresh via `createItem` rather than using `updateDefinition`. If you must update, delete and recreate.

### 5. PySpark Float/Int Type Mismatch in Delta Merges

**Root cause**: Python's `round()` returns `int` for whole numbers; `max(0, float_value)` returns `int` 0. These propagate as `LongType` in Spark.

**Fix**: Always use explicit `float()` casts: `float(round(value, 1))`, `max(0.0, value)`.

### 6. LRO Pattern Is Mandatory for Create Operations

`az rest` returns HTTP 202 with empty body. Extract `x-ms-operation-id` from verbose headers, poll `GET /v1/operations/{id}` until `status == "Succeeded"`.

### 7. Pipeline Create Requires `type` in Body

Include `"type": "DataPipeline"` in the create body alongside `displayName` and `definition`.

### 8. SQL Endpoint Metadata Must Be Synced After ETL

After writing new Delta tables to a lakehouse, the SQL endpoint's metadata cache may be stale. Sync it before querying via SQL:

```powershell
az rest --method post --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/workspaces/$WS_ID/sqlEndpoints/$SQL_EP_ID/refreshMetadata"
```

### 9. Fabric Notebooks Use Native `.py` Format Internally

`getDefinition` returns `notebook-content.py` with `# META {}` and `# CELL` markers — not ipynb.


