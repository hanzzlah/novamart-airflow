import json
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd # type: ignore
from airflow.decorators import dag, task # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

# Airflow 3.x Variables & Dynamic Paths
DATA_ROOT = Path(Variable.get("DATA_ROOT", default_var="/usr/local/airflow"))
BUSINESS_DATE = Variable.get("BUSINESS_DATE", default_var="2024-01-15")
DATE_STR = BUSINESS_DATE.replace("-", "")

LANDING_DIR = DATA_ROOT / "data" / "landing"
RAW_DIR = DATA_ROOT / "data" / "raw" / DATE_STR

REQUIRED_FILES = {
    "sales": f"sales_{DATE_STR}.csv",
    "orders": f"orders_{DATE_STR}.json",
    "shipments": f"shipments_{DATE_STR}.xml",
    "inventory": f"inventory_{DATE_STR}.xlsx",
}


@dag(
    dag_id="ingest_files",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["novamart", "ingest"],
)
def ingest_files_pipeline():

    @task
    def check_landing_inputs():
        """Task 1: Fail clear if any of 4 files are missing."""
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        missing = [f for f in REQUIRED_FILES.values() if not (LANDING_DIR / f).exists()]
        if missing:
            raise FileNotFoundError(f"Missing required landing files: {missing}")

    @task
    def ingest_sales() -> dict:
        """Task 2: Ingest Sales CSV."""
        src, dst = LANDING_DIR / REQUIRED_FILES["sales"], RAW_DIR / REQUIRED_FILES["sales"]
        shutil.copy(src, dst)
        df = pd.read_csv(src)
        return {"file_name": REQUIRED_FILES["sales"], "source_type": "csv", "row_count": len(df), "status": "SUCCESS"}

    @task
    def ingest_orders() -> dict:
        """Task 3: Ingest Orders JSON."""
        src, dst = LANDING_DIR / REQUIRED_FILES["orders"], RAW_DIR / REQUIRED_FILES["orders"]
        shutil.copy(src, dst)
        with open(src, "r") as f:
            data = json.load(f)
        row_count = len(data.get("orders", []))
        return {"file_name": REQUIRED_FILES["orders"], "source_type": "json", "row_count": row_count, "status": "SUCCESS"}

    @task
    def ingest_shipments() -> dict:
        """Task 4: Ingest Shipments XML."""
        src, dst = LANDING_DIR / REQUIRED_FILES["shipments"], RAW_DIR / REQUIRED_FILES["shipments"]
        shutil.copy(src, dst)
        tree = ET.parse(src)
        root = tree.getroot()
        row_count = len(root.findall("shipment"))
        return {"file_name": REQUIRED_FILES["shipments"], "source_type": "xml", "row_count": row_count, "status": "SUCCESS"}

    @task
    def ingest_inventory() -> dict:
        """Task 5: Ingest Inventory Excel ('stock' sheet)."""
        src, dst = LANDING_DIR / REQUIRED_FILES["inventory"], RAW_DIR / REQUIRED_FILES["inventory"]
        shutil.copy(src, dst)
        df = pd.read_excel(src, sheet_name="stock")
        return {"file_name": REQUIRED_FILES["inventory"], "source_type": "excel", "row_count": len(df), "status": "SUCCESS"}

    @task
    def write_manifest(metadata_list: list[dict]):
        """Task 6: Write manifest.csv to raw folder."""
        now_iso = datetime.now(timezone.utc).isoformat()
        for meta in metadata_list:
            meta["ingested_at"] = now_iso
        
        manifest_df = pd.DataFrame(metadata_list)
        manifest_df.to_csv(RAW_DIR / "manifest.csv", index=False)

    # Workflow Dependencies
    check = check_landing_inputs()
    t_sales = ingest_sales()
    t_orders = ingest_orders()
    t_shipments = ingest_shipments()
    t_inventory = ingest_inventory()
    
    manifest = write_manifest([t_sales, t_orders, t_shipments, t_inventory])

    check >> [t_sales, t_orders, t_shipments, t_inventory] >> manifest
    trigger_transform_load = TriggerDagRunOperator(
        task_id="trigger_transform_load",
        trigger_dag_id="transform_load",
        wait_for_completion=False,
    )

    # Set dependency after manifest creation task
    manifest >> trigger_transform_load


ingest_files_pipeline()