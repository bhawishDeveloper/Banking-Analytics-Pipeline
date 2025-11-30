from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.decorators import task

# ---------- config ----------
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))

# Path to SQLite DB 
DB_PATH = Path("/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db")
SQLALCHEMY_CONN = f"sqlite:///{DB_PATH}"

# source table that has raw data
SOURCE_TABLE = "transactions" 
TARGET_TABLE = "transactions_summary"
CSV_OUTPUT = "/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/transactions_summary.csv"
# ---------- end config ----------

default_args = {
    "owner": "bhawish",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_pipeline",
    default_args=default_args,
    description="Extract -> Clean -> Aggregate -> Save pipeline (SQLite, balance column)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["banking", "etl"],
) as dag:
    @task
    def extract(execution_date: datetime = None) -> pd.DataFrame:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"SQLite DB not found at: {DB_PATH}")

        engine = create_engine(SQLALCHEMY_CONN)
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", con=conn)
        finally:
            conn.close()

        # ingestion timestamp
        df["ingest_ts"] = pd.Timestamp.utcnow()

        print("[extract] rows =", len(df))
        print("[extract] columns =", list(df.columns))
        return df

    @task
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the raw data:
        - Ensure 'balance' is numeric
        - Drop rows with null balance
        - Drop negative balance (if you consider them invalid)
        - Drop exact duplicates
        """
        print("[clean] Task started")
        print(f"[clean] received rows = {len(df)}")
        print(f"[clean] columns = {list(df.columns)}")

        if df.empty:
            print("[clean] empty dataframe, nothing to do")
            return df

        if "balance" not in df.columns:
            raise KeyError(
                "Expected 'balance' column in raw data. Columns: " + ", ".join(df.columns)
            )

        df["balance"] = pd.to_numeric(df["balance"], errors="coerce")  # convert to numeric, set errors to NaN
        df = df[df["balance"].notna()].copy()
        df = df[df["balance"] >= 0].copy() # Optionally drop negative balances
        df = df.drop_duplicates()  # Remove exact duplicates
        print(f"[clean] cleaned rows = {len(df)}")
        if len(df) == 0:
            raise ValueError("[clean] No rows left after cleaning – possible data issue.")
        return df

    @task
    def aggregate(df: pd.DataFrame, execution_date: datetime = None) -> dict:
        """
        Aggregate into multiple summary tables:
        1. Daily summary (overall KPIs)
        2. Job-based campaign performance
        """
        print("[aggregate] Task started")
        print(f"[aggregate] Received df with {len(df)} rows")
        
        dt = (
            pd.Timestamp(execution_date).normalize()
            if execution_date
            else pd.Timestamp.utcnow().normalize()
        )
        run_id = f"daily_pipeline_{pd.Timestamp.utcnow().isoformat()}"

        if df.empty:
            daily_summary = pd.DataFrame([{
                "dt": dt,
                "run_id": run_id,
                "total_customers": 0,
                "bal_sum": 0.0,
                "bal_mean": None,
                "bal_min": None,
                "bal_max": None,
            }])
            job_summary = pd.DataFrame()
            return {"daily_summary": daily_summary, "job_campaign_summary": job_summary}
        
        # 1. Daily Summary
        daily_summary = pd.DataFrame([{
            "dt": dt,
            "run_id": run_id,
            "total_customers": len(df),
            "bal_sum": float(df["balance"].sum()),
            "bal_mean": float(df["balance"].mean()),
            "bal_min": float(df["balance"].min()),
            "bal_max": float(df["balance"].max()),
            "deposit_yes_count": int((df["deposit"] == "yes").sum()),
            "deposit_conversion_rate": float((df["deposit"] == "yes").sum() / len(df) * 100),
        }])
        
        # 2. Job-based Campaign Performance
        job_summary = df.groupby("job").agg({ "balance": ["count", "mean", "sum"], "deposit": lambda x: (x == "yes").sum()
        }).reset_index()
        
        job_summary.columns = ["job", "customer_count", "avg_balance", "total_balance", "deposit_yes_count"]
        job_summary["deposit_conversion_rate"] = (job_summary["deposit_yes_count"] / job_summary["customer_count"] * 100).round(2)
        job_summary["dt"] = dt
        job_summary["run_id"] = run_id
        
        print(f"[aggregate] Daily summary: {daily_summary.to_dict(orient='records')}")
        print(f"[aggregate] Job summary rows: {len(job_summary)}")
        
        return {
            "daily_summary": daily_summary,
            "job_campaign_summary": job_summary
        }


    @task
    def save(summary_dict: dict) -> str:
        """ Save multiple summary tables to SQLite and export to CSV."""
        print("[save] Task started")
        print(f"[save] Received {len(summary_dict)} summary tables")
        
        if not DB_PATH.exists():
            raise FileNotFoundError(f"SQLite DB not found at: {DB_PATH}")
        
        engine = create_engine(SQLALCHEMY_CONN)
        conn = engine.raw_connection()
        
        try:
            for table_name, summary_df in summary_dict.items():
                if summary_df.empty:
                    print(f"[save] {table_name} is empty, skipping")
                    continue
                # Save to SQLite
                summary_df.to_sql(
                    table_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                )
                print(f"[save] Wrote {len(summary_df)} row(s) to {table_name}")
                
                # Read back full table
                full_df = pd.read_sql(f"SELECT * FROM {table_name}", con=conn)
                # Export to CSV
                csv_path = f"/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/{table_name}.csv"
                full_df.to_csv(csv_path, index=False)
                print(f"[save] Exported {table_name} to {csv_path}")
        finally:
            conn.close()
        return "ok"

    # Task pipeline
    df_raw = extract()
    df_clean = clean(df_raw)
    summary = aggregate(df_clean)
    result = save(summary)
