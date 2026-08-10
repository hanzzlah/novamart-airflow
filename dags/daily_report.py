# dags/daily_report.py
from datetime import datetime
from airflow.decorators import dag, task # type: ignore
from airflow.models import Variable # type: ignore

from include.report_helpers import build_daily_report, build_low_stock_report


@dag(
    dag_id="daily_report",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["novamart", "reporting"],
)
def daily_report_pipeline():

    @task
    def generate_daily_report(biz_date: str):
        """Task 1: Query metrics from database and write daily_report_YYYYMMDD.csv."""
        build_daily_report(biz_date)

    @task
    def generate_low_stock_report(biz_date: str):
        """Task 2: Query low stock SKUs from database and write low_stock_YYYYMMDD.csv."""
        build_low_stock_report(biz_date)

    # Fetch business date from Variable
    biz_date = Variable.get("BUSINESS_DATE")

    # Execute reporting tasks in parallel
    generate_daily_report(biz_date)
    generate_low_stock_report(biz_date)


daily_report_pipeline()