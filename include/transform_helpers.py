# include/transform_helpers.py
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd # type: ignore

from airflow.models import Variable # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore

DATA_ROOT = Path(Variable.get("DATA_ROOT", default_var="/usr/local/airflow"))
POSTGRES_CONN_ID = "postgres_default"


def get_date_formats(biz_date_str: str) -> tuple[str, str]:
    """
    Parses any input date string and returns a tuple:
    - raw_date: YYYYMMDD (used for file/folder path lookup)
    - sql_date: YYYY-MM-DD (used for storing in databases, files, and reports)
    """
    clean = str(biz_date_str).strip().replace("-", "")
    if len(clean) == 8 and clean.isdigit():
        raw_date = clean
        sql_date = f"{clean[:4]}-{clean[4:6]}-{clean[6:]}"
        return raw_date, sql_date
    return clean, str(biz_date_str)


# -------------------------------------------------------------------
# 1. TRANSFORMATION FUNCTIONS (Tasks 1 - 5)
# -------------------------------------------------------------------

def transform_sales_raw(biz_date: str) -> dict:
    """Task 1: Read raw sales CSV using YYYYMMDD paths, normalize types, compute line_amount."""
    raw_date, sql_date = get_date_formats(biz_date)
    file_name = f"sales_{raw_date}.csv"
    raw_path = DATA_ROOT / "data" / "raw" / raw_date / file_name

    df = pd.read_csv(raw_path)
    records = []

    for _, row in df.iterrows():
        r = row.to_dict()
        qty = float(r["quantity"]) if pd.notna(r.get("quantity")) else None
        price = float(r["unit_price"]) if pd.notna(r.get("unit_price")) else None
        line_amt = (qty * price) if (qty is not None and price is not None) else 0.0

        sale_dt = str(r.get("sale_date", "")).strip()
        if not sale_dt or pd.isna(r.get("sale_date")):
            sale_dt = sql_date
        else:
            _, sale_dt = get_date_formats(sale_dt)

        records.append({
            "sale_id": str(r["sale_id"]).strip() if pd.notna(r.get("sale_id")) else None,
            "store_id": str(r.get("store_id", "")).strip(),
            "product_id": str(r.get("product_id", "")).strip(),
            "quantity": int(qty) if qty is not None else None,
            "unit_price": price,
            "sale_date": sale_dt,
            "payment_method": str(r.get("payment_method", "")).strip(),
            "line_amount": round(line_amt, 2),
            "raw": str(r)
        })

    return {"records": records, "rows_in": len(records), "file_name": file_name}


def transform_orders_raw(biz_date: str) -> dict:
    """Task 2: Read raw orders JSON using YYYYMMDD paths, extract headers, derive business_date as YYYY-MM-DD."""
    raw_date, sql_date = get_date_formats(biz_date)
    file_name = f"orders_{raw_date}.json"
    raw_path = DATA_ROOT / "data" / "raw" / raw_date / file_name

    with open(raw_path, "r") as f:
        payload = json.load(f)

    orders_list = payload.get("orders", payload if isinstance(payload, list) else [])
    records = []

    for o in orders_list:
        order_ts = str(o.get("order_ts", "")).strip()
        derived_biz_date = order_ts[:10] if len(order_ts) >= 10 else sql_date
        _, derived_biz_date = get_date_formats(derived_biz_date)

        records.append({
            "order_id": str(o.get("order_id")).strip() if o.get("order_id") else None,
            "customer_id": str(o.get("customer_id", "")).strip(),
            "order_ts": order_ts,
            "status": str(o.get("status", "")).strip().lower(),
            "total_amount": float(o.get("total_amount", 0.0)),
            "channel": str(o.get("channel", "web")).strip(),
            "business_date": derived_biz_date,
            "items": o.get("items", []),
            "raw": json.dumps(o)
        })

    return {"records": records, "rows_in": len(records), "file_name": file_name}


def extract_and_flatten_order_items(orders_records: list) -> list:
    """Task 3: Flatten nested order items array into individual line items."""
    flattened_items = []
    for o in orders_records:
        order_id = o.get("order_id")
        for item in o.get("items", []):
            qty = int(item.get("quantity", 0))
            price = float(item.get("unit_price", 0.0))
            flattened_items.append({
                "order_id": order_id,
                "product_id": str(item.get("product_id", "")).strip(),
                "quantity": qty,
                "unit_price": price,
                "line_amount": round(qty * price, 2),
                "parent_status": o.get("status")
            })
    return flattened_items


def transform_shipments_raw(biz_date: str) -> dict:
    """Task 4: Read raw shipments XML using YYYYMMDD paths, normalize types and dates to YYYY-MM-DD."""
    raw_date, sql_date = get_date_formats(biz_date)
    file_name = f"shipments_{raw_date}.xml"
    raw_path = DATA_ROOT / "data" / "raw" / raw_date / file_name

    tree = ET.parse(raw_path)
    root = tree.getroot()
    records = []

    for shipment in root.findall("shipment"):
        ship_id = shipment.findtext("shipment_id")
        qty_str = shipment.findtext("quantity")
        qty = int(qty_str) if qty_str and qty_str.isdigit() else (int(qty_str) if qty_str == "0" else None)

        ship_dt = (shipment.findtext("ship_date") or sql_date).strip()
        _, ship_dt = get_date_formats(ship_dt)

        records.append({
            "shipment_id": ship_id.strip() if ship_id else None,
            "supplier_id": (shipment.findtext("supplier_id") or "").strip(),
            "product_id": (shipment.findtext("product_id") or "").strip(),
            "quantity": qty,
            "ship_date": ship_dt,
            "status": (shipment.findtext("status") or "").strip(),
            "raw": ET.tostring(shipment, encoding="unicode")
        })

    return {"records": records, "rows_in": len(records), "file_name": file_name}


def transform_inventory_raw(biz_date: str) -> dict:
    """Task 5: Read raw inventory Excel stock sheet using YYYYMMDD paths, attach snapshot_date as YYYY-MM-DD."""
    raw_date, sql_date = get_date_formats(biz_date)
    file_name = f"inventory_{raw_date}.xlsx"
    raw_path = DATA_ROOT / "data" / "raw" / raw_date / file_name

    df = pd.read_excel(raw_path, sheet_name="stock")
    records = []

    for _, row in df.iterrows():
        r = row.to_dict()
        pid = r.get("product_id")
        wid = r.get("warehouse_id")

        records.append({
            "product_id": str(pid).strip() if pd.notna(pid) else None,
            "product_name": str(r.get("product_name", "")).strip(),
            "warehouse_id": str(wid).strip() if pd.notna(wid) else None,
            "quantity_on_hand": int(r["quantity_on_hand"]) if pd.notna(r.get("quantity_on_hand")) else 0,
            "reorder_level": int(r["reorder_level"]) if pd.notna(r.get("reorder_level")) else 0,
            "snapshot_date": sql_date,
            "raw": str(r)
        })

    return {"records": records, "rows_in": len(records), "file_name": file_name}


# -------------------------------------------------------------------
# 2. DATA QUALITY CHECKS (Task 6)
# -------------------------------------------------------------------

def execute_dq_checks(
    sales_payload: dict,
    orders_payload: dict,
    order_items_records: list,
    shipments_payload: dict,
    inventory_payload: dict,
    biz_date: str
) -> dict:
    """Task 6: Apply Section 4 Quality Rules (AT-03, AT-04, AT-05). Route rejected rows to disk."""
    raw_date, sql_date = get_date_formats(biz_date)
    rejected_dir = DATA_ROOT / "data" / "rejected" / raw_date
    rejected_dir.mkdir(parents=True, exist_ok=True)

    metrics = {}

    # --- Sales DQ (AT-03) ---
    clean_sales, rejected_sales = [], []
    seen_sale_ids = set()

    for r in sales_payload["records"]:
        sale_id = r["sale_id"]
        if not sale_id:
            reason = "MISSING_SALE_ID"
        elif sale_id in seen_sale_ids:
            reason = "DUPLICATE_SALE_ID"
        elif r["quantity"] is None or r["quantity"] <= 0:
            reason = "INVALID_QUANTITY"
        elif r["unit_price"] is None or r["unit_price"] <= 0.0:
            reason = "INVALID_UNIT_PRICE"
        else:
            reason = None

        if reason:
            rejected_sales.append(_build_rejection(sql_date, sales_payload["file_name"], r["raw"], reason))
        else:
            seen_sale_ids.add(sale_id)
            clean_sales.append((
                r["sale_id"], r["store_id"], r["product_id"], r["quantity"],
                r["unit_price"], r["sale_date"], r["payment_method"], r["line_amount"]
            ))

    _write_rejections(rejected_dir / "sales_rejected.csv", rejected_sales)
    metrics["sales"] = _calc_metric(sales_payload["rows_in"], len(clean_sales), len(rejected_sales))

    # --- Orders & Items DQ (AT-04) ---
    clean_orders, rejected_orders = [], []
    valid_order_ids = set()

    for r in orders_payload["records"]:
        if not r["order_id"]:
            reason = "MISSING_ORDER_ID"
        elif r["status"] != "completed":
            reason = f"EXCLUDED_STATUS_{r['status'].upper()}"
        else:
            reason = None

        if reason:
            rejected_orders.append(_build_rejection(sql_date, orders_payload["file_name"], r["raw"], reason))
        else:
            valid_order_ids.add(r["order_id"])
            clean_orders.append((
                r["order_id"], r["customer_id"], r["order_ts"], r["status"],
                r["total_amount"], r["channel"], r["business_date"]
            ))

    _write_rejections(rejected_dir / "orders_rejected.csv", rejected_orders)
    metrics["orders"] = _calc_metric(orders_payload["rows_in"], len(clean_orders), len(rejected_orders))

    clean_items = [
        (i["order_id"], i["product_id"], i["quantity"], i["unit_price"], i["line_amount"])
        for i in order_items_records if i["order_id"] in valid_order_ids
    ]
    metrics["order_items"] = _calc_metric(len(order_items_records), len(clean_items), len(order_items_records) - len(clean_items))

    # --- Shipments DQ (AT-05) ---
    clean_shipments, rejected_shipments = [], []

    for r in shipments_payload["records"]:
        if not r["shipment_id"]:
            reason = "MISSING_SHIPMENT_ID"
        elif r["quantity"] is None or r["quantity"] <= 0:
            reason = "INVALID_QUANTITY_ZERO_OR_LESS"
        else:
            reason = None

        if reason:
            rejected_shipments.append(_build_rejection(sql_date, shipments_payload["file_name"], r["raw"], reason))
        else:
            clean_shipments.append((
                r["shipment_id"], r["supplier_id"], r["product_id"],
                r["quantity"], r["ship_date"], r["status"]
            ))

    _write_rejections(rejected_dir / "shipments_rejected.csv", rejected_shipments)
    metrics["shipments"] = _calc_metric(shipments_payload["rows_in"], len(clean_shipments), len(rejected_shipments))

    # --- Inventory DQ ---
    clean_inventory, rejected_inventory = [], []

    for r in inventory_payload["records"]:
        if not r["product_id"]:
            reason = "MISSING_PRODUCT_ID"
        elif not r["warehouse_id"]:
            reason = "MISSING_WAREHOUSE_ID"
        else:
            reason = None

        if reason:
            rejected_inventory.append(_build_rejection(sql_date, inventory_payload["file_name"], r["raw"], reason))
        else:
            clean_inventory.append((
                r["product_id"], r["product_name"], r["warehouse_id"],
                r["quantity_on_hand"], r["reorder_level"], r["snapshot_date"]
            ))

    _write_rejections(rejected_dir / "inventory_rejected.csv", rejected_inventory)
    metrics["inventory"] = _calc_metric(inventory_payload["rows_in"], len(clean_inventory), len(rejected_inventory))

    return {
        "valid_data": {
            "sales": clean_sales,
            "orders": clean_orders,
            "order_items": clean_items,
            "shipments": clean_shipments,
            "inventory": clean_inventory
        },
        "metrics": metrics
    }


# -------------------------------------------------------------------
# 3. IDEMPOTENT DATABASE LOADING (Task 7)
# -------------------------------------------------------------------

def load_warehouse_tables(valid_data: dict, biz_date: str):
    """Task 7: Delete existing records for YYYY-MM-DD date and bulk insert clean records."""
    _, sql_date = get_date_formats(biz_date)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # 1. Delete existing records for idempotency (AT-08)
    pg_hook.run(f"DELETE FROM order_items WHERE order_id IN (SELECT order_id FROM orders WHERE business_date = '{sql_date}');")
    pg_hook.run(f"DELETE FROM orders WHERE business_date = '{sql_date}';")
    pg_hook.run(f"DELETE FROM sales WHERE sale_date = '{sql_date}';")
    pg_hook.run(f"DELETE FROM shipments WHERE ship_date = '{sql_date}';")
    pg_hook.run(f"DELETE FROM inventory WHERE snapshot_date = '{sql_date}';")

    # 2. Insert valid rows
    if valid_data["sales"]:
        pg_hook.insert_rows(
            table="sales",
            target_fields=["sale_id", "store_id", "product_id", "quantity", "unit_price", "sale_date", "payment_method", "line_amount"],
            rows=valid_data["sales"]
        )

    if valid_data["orders"]:
        pg_hook.insert_rows(
            table="orders",
            target_fields=["order_id", "customer_id", "order_ts", "status", "total_amount", "channel", "business_date"],
            rows=valid_data["orders"]
        )

    if valid_data["order_items"]:
        pg_hook.insert_rows(
            table="order_items",
            target_fields=["order_id", "product_id", "quantity", "unit_price", "line_amount"],
            rows=valid_data["order_items"]
        )

    if valid_data["shipments"]:
        pg_hook.insert_rows(
            table="shipments",
            target_fields=["shipment_id", "supplier_id", "product_id", "quantity", "ship_date", "status"],
            rows=valid_data["shipments"]
        )

    if valid_data["inventory"]:
        pg_hook.insert_rows(
            table="inventory",
            target_fields=["product_id", "product_name", "warehouse_id", "quantity_on_hand", "reorder_level", "snapshot_date"],
            rows=valid_data["inventory"]
        )


# -------------------------------------------------------------------
# 4. DQ SUMMARY PERSISTENCE (Task 8)
# -------------------------------------------------------------------

def persist_dq_summary(metrics: dict, biz_date: str):
    """Task 8: Persist row metrics (rows_in, rows_loaded, rows_rejected) to CSV using YYYYMMDD filename."""
    raw_date, sql_date = get_date_formats(biz_date)
    reports_dir = DATA_ROOT / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    for source, counts in metrics.items():
        summary_rows.append({
            "business_date": sql_date,
            "source": source,
            "rows_in": counts["rows_in"],
            "rows_loaded": counts["rows_loaded"],
            "rows_rejected": counts["rows_rejected"]
        })

    df = pd.DataFrame(summary_rows)
    df.to_csv(reports_dir / f"dq_summary_{raw_date}.csv", index=False)


# -------------------------------------------------------------------
# PRIVATE UTILITIES
# -------------------------------------------------------------------

def _build_rejection(sql_date: str, source_file: str, raw_record: str, reason: str) -> dict:
    return {
        "business_date": sql_date,
        "source_file": source_file,
        "raw_record": raw_record,
        "rejection_reason": reason,
        "rejected_at": datetime.now(timezone.utc).isoformat()
    }


def _write_rejections(file_path: Path, records: list):
    if records:
        pd.DataFrame(records).to_csv(file_path, index=False)


def _calc_metric(rows_in: int, rows_loaded: int, rows_rejected: int) -> dict:
    return {"rows_in": rows_in, "rows_loaded": rows_loaded, "rows_rejected": rows_rejected}