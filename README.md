# Banking-Analytics-Pipeline
End‑to‑End Banking Analytics Pipeline

#### Overview
This project simulates a small data platform for a retail bank using public banking data (bank marketing dataset).  
It focuses on building an automated **daily analytics pipeline** for campaign performance and customer KPIs.

#### Tech Stack
- Python (Pandas)
- Apache Airflow
- SQLite (as the analytics database)
- SQLAlchemy

#### Pipeline (Airflow DAG: `daily_pipeline`)
Daily job with 4 tasks:

1. **extract**
   - Reads raw customer & campaign data from a SQLite database.
   - Adds an ingestion timestamp (`ingest_ts`).

2. **clean**
   - Ensures `balance` is numeric.
   - Drops null / negative balances.
   - Removes duplicate rows.

3. **aggregate**
   - Produces two analytics DataFrames:
     - `daily_summary`  
       - Date (`dt`), run identifier (`run_id`)  
       - Total customers, total balance, average/min/max balance  
       - Deposit campaign conversions and conversion rate
     - `job_campaign_summary`  
       - Job-wise customer counts  
       - Average & total balance per job  
       - Deposit conversions and conversion rate per job  
       - Date (`dt`) and `run_id` for traceability

4. **save**
   - Writes both summaries into SQLite tables:
     - `daily_summary`
     - `job_campaign_summary`
   - Exports both tables as CSV files:
     - `data/daily_summary.csv`
     - `data/job_campaign_summary.csv`

#### Example Output (from `daily_summary.csv`)
- `dt`: 2025-11-29
- `total_customers`: 10,474
- `bal_sum`: 17,284,326
- `bal_mean`: 1,650.21
- `bal_min`: 0
- `bal_max`: 81,204
- `deposit_yes_count`: (number of customers with deposit = "yes")
- `deposit_conversion_rate`: % of customers who subscribed to the deposit

#### How This Helps (Business View)
- **Daily summary**: gives management a quick view of customer base size, balances, and overall campaign performance.
- **Job-based summary**: shows which job segments respond best to deposit campaigns and how valuable they are (balances).

#### Next Steps (Planned)
- Connect a BI tool (Power BI / Looker Studio) to the exported CSVs or database.
- Build dashboards:
  - Trend of daily deposit conversion rate.
  - Conversion rate by job.
  - Customer count and average balance by job.

#### Author
- Bhawish Raj (Data Engineer / Banking Analytics)
