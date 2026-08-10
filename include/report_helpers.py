# include/report_helpers.py
from pathlib import Path
import pandas as pd # type: ignore

from airflow.models import Variable # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

DATA_ROOT = Path(Variable.get("DATA_ROOT", default_var="/usr/local/airflow"))
POSTGRES_CONN_ID = "postgres_default"


def get_date_formats(biz_date_str: str) -> tuple[str, str]:
    """
    Parses input date string and returns a tuple:
    - raw_date: YYYYMMDD (used for file/folder naming)
    - sql_date: YYYY-MM-DD (used for SQL queries)
    """
    clean = str(biz_date_str).strip().replace("-", "")
    if len(clean) == 8 and clean.isdigit():
        raw_date = clean
        sql_date = f"{clean[:4]}-{clean[4:6]}-{clean[6:]}"
        return raw_date, sql_date
    return clean, str(biz_date_str)


def build_daily_report(biz_date: str):
    """Queries core PostgreSQL tables and exports daily_report_YYYYMMDD.csv."""
    raw_date, sql_date = get_date_formats(biz_date)
    reports_dir = DATA_ROOT / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # 1. POS Revenue
    pos_rev_res = pg_hook.get_first(
        f"SELECT COALESCE(SUM(line_amount), 0.0) FROM sales WHERE sale_date = '{sql_date}';"
    )
    pos_revenue = float(pos_rev_res[0]) if pos_rev_res else 0.0

    # 2. Online Revenue (Completed Orders)
    online_rev_res = pg_hook.get_first(
        f"SELECT COALESCE(SUM(total_amount), 0.0) FROM orders WHERE business_date = '{sql_date}' AND status = 'completed';"
    )
    online_revenue = float(online_rev_res[0]) if online_rev_res else 0.0

    # 3. Total Revenue
    total_revenue = pos_revenue + online_revenue

    # 4. POS Transaction Count
    pos_txn_res = pg_hook.get_first(
        f"SELECT COUNT(DISTINCT sale_id) FROM sales WHERE sale_date = '{sql_date}';"
    )
    pos_txn_count = int(pos_txn_res[0]) if pos_txn_res else 0

    # 5. Online Completed Order Count
    online_cnt_res = pg_hook.get_first(
        f"SELECT COUNT(order_id) FROM orders WHERE business_date = '{sql_date}' AND status = 'completed';"
    )
    online_completed_count = int(online_cnt_res[0]) if online_cnt_res else 0

    # 6. Shipments Delivered
    ship_res = pg_hook.get_first(
        f"SELECT COUNT(shipment_id) FROM shipments WHERE ship_date = '{sql_date}' AND status = 'delivered';"
    )
    shipments_delivered = int(ship_res[0]) if ship_res else 0

    # 7. Low Stock SKU Count
    low_stock_cnt_res = pg_hook.get_first(
        f"SELECT COUNT(DISTINCT product_id) FROM inventory WHERE snapshot_date = '{sql_date}' AND quantity_on_hand < reorder_level;"
    )
    low_stock_count = int(low_stock_cnt_res[0]) if low_stock_cnt_res else 0

    metrics_data = [
        {"metric": "pos_revenue", "value": round(pos_revenue, 2)},
        {"metric": "online_revenue_completed", "value": round(online_revenue, 2)},
        {"metric": "total_revenue", "value": round(total_revenue, 2)},
        {"metric": "pos_txn_count", "value": pos_txn_count},
        {"metric": "online_completed_order_count", "value": online_completed_count},
        {"metric": "shipments_delivered", "value": shipments_delivered},
        {"metric": "low_stock_sku_count", "value": low_stock_count},
    ]

    df = pd.DataFrame(metrics_data)
    out_path = reports_dir / f"daily_report_{raw_date}.csv"
    df.to_csv(out_path, index=False)


def build_low_stock_report(biz_date: str):
    """Queries inventory table and exports low_stock_YYYYMMDD.csv."""
    raw_date, sql_date = get_date_formats(biz_date)
    reports_dir = DATA_ROOT / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql_query = f"""
        SELECT 
            snapshot_date AS business_date,
            product_id,
            product_name,
            warehouse_id,
            quantity_on_hand,
            reorder_level
        FROM inventory
        WHERE snapshot_date = '{sql_date}'
          AND quantity_on_hand < reorder_level;
    """

    df = pg_hook.get_pandas_df(sql_query)

    # Ensure correct column headers even when DataFrame is empty
    cols = ["business_date", "product_id", "product_name", "warehouse_id", "quantity_on_hand", "reorder_level"]
    if df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        df = df[cols]

    out_path = reports_dir / f"low_stock_{raw_date}.csv"
    df.to_csv(out_path, index=False)