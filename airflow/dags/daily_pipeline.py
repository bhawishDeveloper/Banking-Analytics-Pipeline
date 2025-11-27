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
# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))
# # path to your sqlite DB relative to AIRFLOW_HOME/data/banking.db
# DB_PATH = Path(AIRFLOW_HOME) / "data" / "banking.db"
# SQLALCHEMY_CONN = f"sqlite:///{DB_PATH}"
# ---------- end config ----------


# ---------- config ----------
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))
# WRONG: This adds airflow/data/ to the path
# DB_PATH = Path(AIRFLOW_HOME) / "data" / "banking.db"

# CORRECT: Use the actual path where your banking.db file is located
DB_PATH = Path("/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db")
SQLALCHEMY_CONN = f"sqlite:///{DB_PATH}"
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
    description="Extract -> Clean -> Aggregate -> Save pipeline (SQLite)",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 20),
    catchup=False,
    tags=["banking", "etl"],
) as dag:

    @task
    def extract(execution_date: datetime = None) -> pd.DataFrame:
        """
        Read raw transactions from SQLite into a pandas DataFrame.
        """
        DB_PATH_parent = DB_PATH
        if not DB_PATH_parent.exists():
            raise FileNotFoundError(f"SQLite DB not found at: {DB_PATH_parent}")

        engine = create_engine(SQLALCHEMY_CONN)
        
        # Get the raw connection that pandas can use
        conn = engine.raw_connection()
        try:
            df = pd.read_sql("SELECT * FROM transactions", con=conn)
            # Add an execution_date column for traceability (optional)
            df["ingest_ts"] = pd.Timestamp.utcnow()
            # Log basic info
            print(f"[extract] rows={len(df)} columns={list(df.columns)[:10]}")
            return df
        finally:
            conn.close()
        
        # """Extract task - simplified"""
        # print("[extract] Task started successfully!")
        # print(f"[extract] DB_PATH = {DB_PATH}")
        # print(f"[extract] DB exists? {DB_PATH.exists()}")
        
        # # Create a dummy dataframe to pass to next task
        # df = pd.DataFrame({
        #     'Amount': [100.0, 200.0, 50.0],
        #     'transaction_id': [1, 2, 3]
        # })
        # print(f"[extract] Created dummy df with {len(df)} rows")
        # return df

    @task
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Simple cleaning:
        - ensure amount numeric
        - filter missing/negative amounts (example rule)
        - drop exact duplicates
        """
        # if df.empty:
        #     print("[clean] empty dataframe received")
        #     return df

        # # Ensure column names match your CSV (adjust field names if different)
        # # Common field name in credit card fraud dataset: 'Amount'
        # # Try both lowercase and title-case
        # possible_amount_cols = [c for c in df.columns if c.lower() in ("amount", "amt", "transaction_amount")]
        # if not possible_amount_cols:
        #     # If no amount column found, try 'Amount' explicitly and raise helpful error
        #     raise KeyError("No 'amount' column found in transactions table. Columns: " + ", ".join(df.columns))

        # amount_col = possible_amount_cols[0]
        # df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")
        # # drop rows with null amount
        # df = df[df[amount_col].notna()].copy()
        # # filter negative amounts (domain rule — change if you need negatives)
        # df = df[df[amount_col] >= 0].copy()
        # # drop exact duplicates
        # df = df.drop_duplicates()
        # print(f"[clean] cleaned rows={len(df)}")
        # return df

        """Clean task - simplified"""
        print("[clean] Task started successfully!")
        print(f"[clean] Received df with {len(df)} rows")
        print(f"[clean] Columns: {list(df.columns)}")
        return df

    @task
    def aggregate(df: pd.DataFrame, execution_date: datetime = None) -> pd.DataFrame:
        """
        Aggregate into a summary per execution_date.
        We compute: count, sum, mean, min, max of amounts.
        """
        # if df.empty:
        #     # return an empty dataframe with the summary schema
        #     summary = pd.DataFrame(
        #         [
        #             {
        #                 "dt": pd.Timestamp(execution_date).normalize() if execution_date else pd.Timestamp.utcnow().normalize(),
        #                 "tx_count": 0,
        #                 "tx_sum": 0.0,
        #                 "tx_mean": None,
        #                 "tx_min": None,
        #                 "tx_max": None,
        #             }
        #         ]
        #     )
        #     print("[aggregate] empty input -> returning empty summary")
        #     return summary

        # # detect the amount column again
        # amount_col = [c for c in df.columns if c.lower() in ("amount", "amt", "transaction_amount")][0]
        # # perform aggregation (single daily row)
        # total = df[amount_col].sum()
        # count = df[amount_col].count()
        # mean = df[amount_col].mean()
        # mn = df[amount_col].min()
        # mx = df[amount_col].max()

        # summary = pd.DataFrame(
        #     [
        #         {
        #             "dt": pd.Timestamp(execution_date).normalize() if execution_date else pd.Timestamp.utcnow().normalize(),
        #             "tx_count": int(count),
        #             "tx_sum": float(total),
        #             "tx_mean": float(mean) if pd.notna(mean) else None,
        #             "tx_min": float(mn) if pd.notna(mn) else None,
        #             "tx_max": float(mx) if pd.notna(mx) else None,
        #         }
        #     ]
        # )
        # print(f"[aggregate] summary={summary.to_dict(orient='records')}")
        # return summary
        """Aggregate task - simplified"""
        print("[aggregate] Task started successfully!")
        print(f"[aggregate] Received df with {len(df)} rows")
        
        # Create dummy summary
        summary = pd.DataFrame([{
            "dt": pd.Timestamp.utcnow().normalize(),
            "tx_count": len(df),
            "tx_sum": df['Amount'].sum(),
            "tx_mean": df['Amount'].mean(),
            "tx_min": df['Amount'].min(),
            "tx_max": df['Amount'].max(),
        }])
        print(f"[aggregate] Created summary: {summary.to_dict(orient='records')}")
        return summary

    @task
    def save(summary_df: pd.DataFrame) -> str:
        """
        Save aggregated summary to transactions_clean table in SQLite DB,
        and export full table to CSV for easy access in Windows.
        """
        # try:
        #     # Use the correct WSL path
        #     db_path = "/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db"
        #     engine = create_engine(f"sqlite:///{db_path}")
            
        #     # Get raw connection
        #     conn = engine.raw_connection()
            
        #     try:
        #         # 1️⃣ Append summary row to SQLite table
        #         summary_df.to_sql(
        #             "transactions_clean",
        #             con=conn,
        #             if_exists="append",
        #             index=False
        #         )
        #         print(f"[save] wrote {len(summary_df)} row(s) to transactions_clean")

        #         # 2️⃣ Read back the full table
        #         full_df = pd.read_sql("SELECT * FROM transactions_clean", con=conn)
        #     finally:
        #         conn.close()

        #     # 3️⃣ Export to CSV
        #     output_dir = "/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data"
        #     os.makedirs(output_dir, exist_ok=True)

        #     output_file = os.path.join(output_dir, "transactions_clean.csv")
        #     full_df.to_csv(output_file, index=False)

        #     print(f"[save] exported full table to CSV: {output_file}")

        #     return "ok"
            
        # except Exception as e:
        #     print(f"[save] ERROR: {str(e)}")
        #     raise     
        """Save task - simplified"""
        print("[save] Task started successfully!")
        print(f"[save] Received summary with {len(summary_df)} rows")
        print(f"[save] Summary: {summary_df.to_dict(orient='records')}")
        print("[save] Would write to database and export CSV here")
        return "ok"                          

    # @task
    # def verify() -> str:
    #     db_path = "/mnt/c/Users/Raj/OneDrive - Åbo Akademi O365/banking/data/banking.db"
    #     engine = create_engine(f"sqlite:///{db_path}")
        
    #     row_count = pd.read_sql("SELECT COUNT(*) as cnt FROM transactions_clean", con=engine)
    #     print(f"[verify] transactions_clean has {row_count['cnt'][0]} rows")
    #     return "verified"


    # Task pipeline - This should also be inside the DAG context
    df_raw = extract()
    df_clean = clean(df_raw)
    summary = aggregate(df_clean)
    result = save(summary)
