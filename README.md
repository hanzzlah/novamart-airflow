# NovaMart Automated Data Warehouse Pipeline

An enterprise-grade, multi-stage Data Engineering pipeline built with **Apache Airflow**, **Python**, **Pandas**, and **PostgreSQL**. The platform extracts multi-format raw source data (CSV, JSON, XML, Excel), enforces strict Data Quality (DQ) rules with quarantine isolation, idempotently loads core analytical tables in PostgreSQL, and publishes daily executive reporting artifacts.

## 📸 Architectural Overview & Visuals

![System Architecture](./images/pipeline.png)
*Figure 1: High-Level System Architecture and Automated Pipeline Execution Flow.*


## 🎯 Engineering Highlights & Key Features

* **Multi-Format Ingestion**: Ingests and processes 4 distinct source formats: CSV (POS Sales), nested JSON (E-commerce Orders), XML (Shipments), and Excel (Inventory snapshots).
* **Automated Data Quality (DQ) Engine**: Evaluates records against strict validation gates. Corrupted, invalid, or duplicate records are quarantined to `data/rejected/<YYYYMMDD>/` without failing the pipeline run.
* **Strict Idempotency**: Built with a deterministic delete-and-load pattern. Re-running any historical business batch produces identical database state without duplicate fact records.
* **Event-Driven Orchestration**: Automated cross-DAG execution chained across three modular DAGs via `TriggerDagRunOperator`.
* **Modular Code Base**: Separation of Airflow DAG definitions (`dags/`) from core transformation, helper functions, and database interactions (`include/`).

## 🏗 Pipeline DAG Execution Graph

The pipeline relies on three loosely coupled Airflow DAGs that trigger sequentially upon successful task completion:

```mermaid
graph TD
    subgraph DAG1 [ingest_files]
        A[check_landing_files] --> B[copy_to_raw]
        B --> C[create_manifest]
        C --> D[trigger_transform_load]
    end

    subgraph DAG2 [transform_load]
        E[transform_sales]
        F[transform_orders] --> G[flatten_order_items]
        H[transform_shipments]
        I[transform_inventory]

        E & F & G & H & I --> J[run_dq_checks]
        J --> K[load_core_tables]
        J --> L[dq_summary]
        K & L --> M[trigger_daily_report]
    end

    subgraph DAG3 [daily_report]
        N[generate_daily_report]
        O[generate_low_stock_report]
    end

    D --> E & F & H & I
    M --> N & O
```

## 🗄 Core Warehouse Schema (PostgreSQL)

```mermaid
erDiagram
    SALES {
        string sale_id PK
        string store_id
        string product_id
        int quantity
        float unit_price
        date sale_date
        string payment_method
        float line_amount
    }
    ORDERS {
        string order_id PK
        string customer_id
        timestamp order_ts
        string status
        float total_amount
        string channel
        date business_date
    }
    ORDER_ITEMS {
        string order_id FK
        string product_id
        int quantity
        float unit_price
        float line_amount
    }
    SHIPMENTS {
        string shipment_id PK
        string supplier_id
        string product_id
        int quantity
        date ship_date
        string status
    }
    INVENTORY {
        string product_id PK
        string product_name
        string warehouse_id PK
        int quantity_on_hand
        int reorder_level
        date snapshot_date
    }

    ORDERS ||--|{ ORDER_ITEMS : contains
```

## 📋 Data Quality & Acceptance Criteria Coverage

| **ID** | **Title** | **Description / Rule** | **Implementation Location** |
| :--- | :--- | :--- | :--- |
| **AT-01** | Landing Ingestion | Verify file existence and archive raw files to `data/raw/<YYYYMMDD>/` with `manifest.csv`. | `dags/ingest_files.py` |
| **AT-02** | File Failure Alert | Fail ingestion task if any mandatory source file is missing. | `dags/ingest_files.py` |
| **AT-03** | Sales DQ | Reject `quantity <= 0`, `unit_price <= 0`, or duplicate `sale_id`. Compute `line_amount`. | `include/transform_helpers.py` |
| **AT-04** | Orders DQ | Extract top-level header and flatten item array. Exclude `status == 'cancelled'`. | `include/transform_helpers.py` |
| **AT-05** | Shipments DQ | Reject `quantity <= 0`. Normalize shipment date format. | `include/transform_helpers.py` |
| **AT-06** | Inventory DQ | Filter SKUs where `quantity_on_hand < reorder_level` for low-stock reporting. | `include/report_helpers.py` |
| **AT-07** | Report Metrics | Generate `daily_report_<YYYYMMDD>.csv` matching metric contract. | `include/report_helpers.py` |
| **AT-08** | Idempotency | Perform `DELETE WHERE business_date = sql_date` before bulk load into PostgreSQL. | `include/transform_helpers.py` |
| **AT-09** | Airflow Variable | Use `BUSINESS_DATE` Airflow Variable for execution date resolution. | All DAGs |
| **AT-10** | DAG Dependency | Automatically trigger downstream DAGs via `TriggerDagRunOperator`. | `dags/transform_load.py` |

## 📁 Repository Directory Layout

```text
.
├── dags/
│   ├── ingest_files.py          # DAG 1: Validates landing data & generates manifest
│   ├── transform_load.py        # DAG 2: Transforms multi-format data, runs DQ, loads Postgres
│   └── daily_report.py          # DAG 3: Generates executive CSV reports & stock alerts
├── include/
│   ├── transform_helpers.py     # Extraction, DQ filtering, and database bulk load functions
│   └── report_helpers.py        # SQL metric extraction and report generator logic
├── images/
│   ├── pipeline.png             # Pipeline architecture visual
│   └── dashboard.png            # Executive analytics visual mockup
├── data/                        # Mounted Local Data Volume (Ignored by Git)
│   ├── landing/                 # Incoming batch landing drop-zone
│   ├── raw/                     # Archived raw files per date (YYYYMMDD)
│   ├── rejected/                # Quarantined bad data records with rejection reasons
│   └── reports/                 # Executive report artifacts & DQ summaries
├── .env                         # Environment variables & DB connection string
├── Dockerfile                   # Astro CLI Airflow container definition
├── requirements.txt             # Python dependencies (e.g., apache-airflow-providers-postgres)
└── README.md                    # System documentation
```

## 🚀 Environment Setup & Deployment

### 1. Prerequisites

* **Docker Desktop** installed and running.
* **Astro CLI** installed (`brew install astro` or via binary).

### 2. Quickstart Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/novamart-airflow-pipeline.git
   cd novamart-airflow-pipeline
   ```

2. **Configure Environment Variables**:
   Ensure `.env` exists in the project root folder with the Postgres connection string:
   ```env
   AIRFLOW_CONN_POSTGRES_DEFAULT='postgresql://postgres:postgres@postgres:5432/postgres'
   ```

3. **Start Airflow Stack**:
   ```bash
   astro dev start
   ```

4. **Verify Airflow Variables**:
   Open Airflow UI (`http://localhost:8080`) -> Navigate to **Admin** -> **Variables**:
   * `BUSINESS_DATE`: `20240115` (Format: `YYYYMMDD`)
   * `DATA_ROOT`: `/usr/local/airflow`

## 🔄 How to Run Batches (Step-by-Step)

To run a complete end-to-end data batch (e.g., Batch 01: `20240115` or Batch 02: `20240116`):

### Step 1: Stage Landing Data

Copy raw files into `data/landing/`:
* `pos_sales.csv`
* `online_orders.json`
* `shipments.xml`
* `inventory.xlsx`

### Step 2: Set Business Date Variable

In Airflow UI (**Admin -> Variables**), set `BUSINESS_DATE` to match your target date (e.g., `20240115`).

### Step 3: Trigger Ingestion

1. Go to **DAGs** tab.
2. Unpause all three DAGs (`ingest_files`, `transform_load`, `daily_report`).
3. Click **Play** (Trigger DAG) on **`ingest_files`**.

### Step 4: Monitor Trigger Chain

* **`ingest_files`** runs, archives landing files to `data/raw/20240115/`, creates `manifest.csv`, and triggers `transform_load`.
* **`transform_load`** extracts, transforms, quarantines bad rows to `data/rejected/20240115/`, loads clean rows to PostgreSQL, creates `dq_summary_20240115.csv`, and triggers `daily_report`.
* **`daily_report`** queries PostgreSQL and outputs final CSVs to `data/reports/`.

## 📊 Verification & Output Artifact Contracts

Upon successful completion of batch processing, verify the following generated files in `data/reports/`:

### 1. Executive Daily Report (`data/reports/daily_report_20240116.csv`)

```csv
metric,value
pos_revenue,655.0
online_revenue_completed,295.0
total_revenue,950.0
pos_txn_count,8
online_completed_order_count,3
shipments_delivered,2
low_stock_sku_count,1
```

### 2. Low-Stock Alert Report (`data/reports/low_stock_20240116.csv`)

```csv
business_date,product_id,product_name,warehouse_id,quantity_on_hand,reorder_level
2024-01-16,P300,Laptop Stand,W2,18,20
```

### 3. Data Quality Summary (`data/reports/dq_summary_20240116.csv`)

```csv
source,rows_in,rows_loaded,rows_rejected
sales,10,8,2
orders,5,3,2
shipments,4,3,1
inventory,5,5,0
```

## 👤 Author & Contact

**Rana Hanzlah Yasir**  
*Software Engineer | Python Backend & Data Engineering*  
* **LinkedIn**: [linkedin.com/in/hanzlah-yasir](https://linkedin.com/in/hanzlah-yasir)
* **GitHub**: [github.com/hanzzlah](https://github.com/hanzzlah)
