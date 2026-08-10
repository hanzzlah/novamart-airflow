# dags/transform_load.py
from datetime import datetime
from airflow.decorators import dag, task # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

from include.transform_helpers import (
    transform_sales_raw,
    transform_orders_raw,
    extract_and_flatten_order_items,
    transform_shipments_raw,
    transform_inventory_raw,
    execute_dq_checks,
    load_warehouse_tables,
    persist_dq_summary,
)


@dag(
    dag_id="transform_load",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["novamart", "transform", "load"],
)
def transform_load_pipeline():

    @task
    def transform_sales(biz_date: str) -> dict:
        """Task 1: Read raw sales CSV, normalize types, compute line_amount."""
        return transform_sales_raw(biz_date)

    @task
    def transform_orders(biz_date: str) -> dict:
        """Task 2: Read raw orders JSON, extract headers, derive business_date."""
        return transform_orders_raw(biz_date)

    @task
    def flatten_order_items(orders_payload: dict) -> list:
        """Task 3: Flatten nested order items array into individual line items."""
        return extract_and_flatten_order_items(orders_payload["records"])

    @task
    def transform_shipments(biz_date: str) -> dict:
        """Task 4: Read raw shipments XML, normalize types and dates."""
        return transform_shipments_raw(biz_date)

    @task
    def transform_inventory(biz_date: str) -> dict:
        """Task 5: Read raw inventory Excel stock sheet, attach snapshot_date."""
        return transform_inventory_raw(biz_date)

    @task
    def run_dq_checks(
        sales_data: dict,
        orders_data: dict,
        order_items_data: list,
        shipments_data: dict,
        inventory_data: dict,
        biz_date: str,
    ) -> dict:
        """Task 6: Apply Section 4 Quality Rules (AT-03, AT-04, AT-05). Route rejected rows to disk."""
        return execute_dq_checks(
            sales_payload=sales_data,
            orders_payload=orders_data,
            order_items_records=order_items_data,
            shipments_payload=shipments_data,
            inventory_payload=inventory_data,
            biz_date=biz_date,
        )

    @task
    def load_core_tables(dq_results: dict, biz_date: str):
        """Task 7: Delete existing records for biz_date and bulk insert clean records."""
        load_warehouse_tables(dq_results["valid_data"], biz_date)

    @task
    def dq_summary(dq_results: dict, biz_date: str):
        """Task 8: Persist row metrics (rows_in, rows_loaded, rows_rejected) to CSV."""
        persist_dq_summary(dq_results["metrics"], biz_date)

    # Fetch dynamic execution date from Airflow Variable
    biz_date = Variable.get("BUSINESS_DATE")

    # 1. Parallel Extraction & Transformations (Tasks 1-5)
    t1_sales = transform_sales(biz_date)
    t2_orders = transform_orders(biz_date)
    t3_items = flatten_order_items(t2_orders)
    t4_shipments = transform_shipments(biz_date)
    t5_inventory = transform_inventory(biz_date)

    # 2. Centralized DQ Rule Evaluation (Task 6)
    t6_dq = run_dq_checks(
        sales_data=t1_sales,
        orders_data=t2_orders,
        order_items_data=t3_items,
        shipments_data=t4_shipments,
        inventory_data=t5_inventory,
        biz_date=biz_date,
    )

    # 3. Parallel Loading & Reporting (Tasks 7 & 8)
    t7_load = load_core_tables(dq_results=t6_dq, biz_date=biz_date)
    t8_summary = dq_summary(dq_results=t6_dq, biz_date=biz_date)

    trigger_daily_report = TriggerDagRunOperator(
        task_id="trigger_daily_report",
        trigger_dag_id="daily_report",
        wait_for_completion=False,
    )

    # Update dependencies at bottom
    t6_dq >> [t7_load, t8_summary] >> trigger_daily_report


transform_load_pipeline()