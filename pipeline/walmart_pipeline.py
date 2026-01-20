import os
import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.dq_framework.engine import load_yaml, run_checks

DATA_PATH = "/opt/airflow/data/walmart_sales.csv"
CONFIG_PATH = "/opt/airflow/config/walmart_rules.yaml"

STAGING_DIR = "/opt/airflow/warehouse/staging"
WAREHOUSE_DB = "/opt/airflow/warehouse/walmart_dwh.db"

os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs("/opt/airflow/warehouse", exist_ok=True)

DIM_DATE_CSV = os.path.join(STAGING_DIR, "dim_date.csv")
DIM_STORE_CSV = os.path.join(STAGING_DIR, "dim_store.csv")
FACT_SALES_CSV = os.path.join(STAGING_DIR, "fact_sales.csv")


def task_data_quality():
    df = pd.read_csv(DATA_PATH)
    config = load_yaml(CONFIG_PATH)
    report = run_checks(df, dataset_name="walmart_raw", config=config, verbose=True)
    if report.get("warnings"):
        print("DQ warnings:", report["warnings"])
    print(" DQ PASS")


def task_transform():
    df = pd.read_csv(DATA_PATH)

    # Transform
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Date"])

    df["date_id"] = df["Date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["Date"].dt.year
    df["month"] = df["Date"].dt.month
    df["week"] = df["Date"].dt.isocalendar().week.astype(int)

    # Dimensions
    dim_date = df[["date_id", "Date", "year", "month", "week"]].drop_duplicates()
    dim_date.columns = ["date_id", "full_date", "year", "month", "week"]

    dim_store = df[["Store"]].drop_duplicates()
    dim_store.columns = ["store_id"]

    # Fact
    fact_sales = df[[
        "date_id", "Store", "Weekly_Sales", "Holiday_Flag",
        "Temperature", "Fuel_Price", "CPI", "Unemployment"
    ]].copy()

    fact_sales.columns = [
        "date_id", "store_id", "weekly_sales", "holiday_flag",
        "temperature", "fuel_price", "cpi", "unemployment"
    ]

    # Save staging files (so tasks are independent)
    dim_date.to_csv(DIM_DATE_CSV, index=False)
    dim_store.to_csv(DIM_STORE_CSV, index=False)
    fact_sales.to_csv(FACT_SALES_CSV, index=False)

    print(" Transform done. Staging files written.")


def task_load():
    dim_date = pd.read_csv(DIM_DATE_CSV)
    dim_store = pd.read_csv(DIM_STORE_CSV)
    fact_sales = pd.read_csv(FACT_SALES_CSV)

    engine = create_engine(f"sqlite:///{WAREHOUSE_DB}")

    with engine.begin() as conn:
        # Rebuild dimensions each run to avoid duplicates
        dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
        dim_store.to_sql("dim_store", conn, if_exists="replace", index=False)

        # Incremental load for fact (date_id, store_id)
        table_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='fact_sales'")
        ).fetchone()

        if not table_exists:
            fact_sales.to_sql("fact_sales", conn, if_exists="replace", index=False)
            print(" First load: fact_sales created")
        else:
            existing_keys = pd.read_sql_query("SELECT date_id, store_id FROM fact_sales", conn)
            merged = fact_sales.merge(existing_keys, on=["date_id", "store_id"], how="left", indicator=True)
            new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

            if len(new_rows) > 0:
                new_rows.to_sql("fact_sales", conn, if_exists="append", index=False)
                print(f"Incremental load: inserted {len(new_rows)} new rows")
            else:
                print(" Incremental load: no new rows")

    print(f"✅ Load finished. Warehouse DB: {WAREHOUSE_DB}")
