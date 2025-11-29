# dags/daily_pipeline.py
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

# Path to your SQLite DB – adjust ONLY if you moved the file
DB_PATH = Path("/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db")
SQLALCHEMY_CONN = f"sqlite:///{DB_PATH}"

# Name of the source table in SQLite that has your raw data
SOURCE_TABLE = "transactions"   # change if your table is named differently
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
        """
        Read raw data from SQLite into a pandas DataFrame.
        Expected columns:
        age, job, marital, education, default, balance, housing, loan,
        contact, day, month, duration, campaign, pdays, previous,
        poutcome, deposit
        """
        if not DB_PATH.exists():
            raise FileNotFoundError(f"SQLite DB not found at: {DB_PATH}")

        engine = create_engine(SQLALCHEMY_CONN)
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", con=conn)
        finally:
            conn.close()

        # Add ingestion timestamp
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
                "Expected 'balance' column in raw data. Columns: "
                + ", ".join(df.columns)
            )

        # Make sure balance is numeric
        df["balance"] = pd.to_numeric(df["balance"], errors="coerce")

        # Drop null balances
        df = df[df["balance"].notna()].copy()

        # Optionally drop negative balances (adjust if you need them)
        df = df[df["balance"] >= 0].copy()

        # Remove exact duplicates
        df = df.drop_duplicates()

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
        job_summary = df.groupby("job").agg({
            "balance": ["count", "mean", "sum"],
            "deposit": lambda x: (x == "yes").sum()
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
        """
        Save multiple summary tables to SQLite and export to CSV.
        """
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
    # @task
    # def aggregate(df: pd.DataFrame, execution_date: datetime = None) -> pd.DataFrame:
    #     """
    #     Aggregate the cleaned data to a daily summary using 'balance' as the metric.
    #     Produces one row per DAG run with:
    #     - dt        : date (normalized)
    #     - row_count : total number of rows
    #     - bal_sum   : sum of balance
    #     - bal_mean  : mean balance
    #     - bal_min   : min balance
    #     - bal_max   : max balance
    #     """
    #     print("[aggregate] Task started")
    #     print(f"[aggregate] received rows = {len(df)}")
    #     print(f"[aggregate] columns = {list(df.columns)}")

    #     dt = (
    #         pd.Timestamp(execution_date).normalize()
    #         if execution_date
    #         else pd.Timestamp.utcnow().normalize()
    #     )

    #     if df.empty:
    #         summary = pd.DataFrame(
    #             [
    #                 {
    #                     "dt": dt,
    #                     "row_count": 0,
    #                     "bal_sum": 0.0,
    #                     "bal_mean": None,
    #                     "bal_min": None,
    #                     "bal_max": None,
    #                 }
    #             ]
    #         )
    #         print("[aggregate] empty input -> returning empty summary")
    #         return summary

    #     if "balance" not in df.columns:
    #         raise KeyError(
    #             "Expected 'balance' column in cleaned data. Columns: "
    #             + ", ".join(df.columns)
    #         )

    #     total = df["balance"].sum()
    #     count = df["balance"].count()
    #     mean = df["balance"].mean()
    #     mn = df["balance"].min()
    #     mx = df["balance"].max()

    #     summary = pd.DataFrame(
    #         [
    #             {
    #                 "dt": dt,
    #                 "row_count": int(count),
    #                 "bal_sum": float(total),
    #                 "bal_mean": float(mean) if pd.notna(mean) else None,
    #                 "bal_min": float(mn) if pd.notna(mn) else None,
    #                 "bal_max": float(mx) if pd.notna(mx) else None,
    #             }
    #         ]
    #     )
    #     print(f"[aggregate] summary = {summary.to_dict(orient='records')}")
    #     return summary

    # @task
    # def save(summary_df: pd.DataFrame) -> str:
    #     """
    #     Append the summary row into SQLite table TARGET_TABLE
    #     and export the full summary table to CSV for inspection.
    #     """
    #     print("[save] Task started")
    #     print(f"[save] summary rows = {len(summary_df)}")
    #     print(f"[save] summary = {summary_df.to_dict(orient='records')}")

    #     if summary_df.empty:
    #         print("[save] summary is empty, nothing to write")
    #         return "empty"

    #     if not DB_PATH.exists():
    #         raise FileNotFoundError(f"SQLite DB not found at: {DB_PATH}")

    #     engine = create_engine(SQLALCHEMY_CONN)
    #     conn = engine.raw_connection()
    #     try:
    #         # Append summary row(s)
    #         summary_df.to_sql(
    #             TARGET_TABLE,
    #             con=conn,
    #             if_exists="append",
    #             index=False,
    #         )
    #         print(f"[save] wrote {len(summary_df)} row(s) to {TARGET_TABLE}")

    #         # Read back full summary table
    #         full_df = pd.read_sql(f"SELECT * FROM {TARGET_TABLE}", con=conn)
    #     finally:
    #         conn.close()

    #     # Export summary to CSV for Windows access
    #     out_dir = os.path.dirname(CSV_OUTPUT)
    #     os.makedirs(out_dir, exist_ok=True)
    #     full_df.to_csv(CSV_OUTPUT, index=False)
    #     print(f"[save] exported full summary to CSV: {CSV_OUTPUT}")

    #     return "ok"

    # Task pipeline
    df_raw = extract()
    df_clean = clean(df_raw)
    summary = aggregate(df_clean)
    result = save(summary)
