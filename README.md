### End-to-End Banking Analytics Pipeline

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Tech](https://img.shields.io/badge/stack-Python%20%7C%20Airflow%20%7C%20SQL-blue)

#### 1. Project Overview

This project simulates a small **banking analytics platform** using public bank marketing data.  
It focuses on building an automated **daily data pipeline** that prepares clean, analytics-ready tables for dashboards.

The pipeline is orchestrated with **Apache Airflow** and generates:
- A **daily KPIs summary** (overall customer & campaign metrics)
- A **job-based campaign performance summary** (conversion by customer job type)

These outputs are designed to be consumed by a BI tool such as **Power BI**.

---

#### 2. Tech Stack

- **Language:** Python (Pandas)
- **Orchestration:** Apache Airflow
- **Storage:** SQLite (simulating an analytics DB)
- **ORM / DB Access:** SQLAlchemy
- **Version Control:** Git + GitHub
- **Planned BI Layer:** Power BI (using the exported CSVs)

---

#### 3. Architecture

High-level flow Diagram:

```text
┌──────────────────────────┐
│  Raw Banking Data (DB)   │
│  (bank marketing table)  │
└─────────────┬────────────┘
              │
              ▼
      ┌────────────────┐
      │  Airflow DAG   │
      │  daily_pipeline│
      └────────────────┘
              │
   ┌──────────┼──────────┐
   ▼          ▼          ▼
[extract]  [clean]   [aggregate]
                        │
                        ▼
                  [save results]
                        │
                        ▼
           ┌──────────────────────────┐
           │  SQLite summary tables   │
           │  & exported CSV files    │
           └───────────┬──────────────┘
                       ▼
          Power BI / Dashboard (planned)
```

> A colorful version of this diagram (PNG) can be added as `architecture.png` in the repo.

---

#### 4. Data & Business Context

The dataset represents a **bank marketing campaign** where customers are contacted and asked to subscribe to a long-term deposit.

Key columns used in this project (examples):

- `age` – customer age  
- `job` – job type (admin., technician, services, etc.)  
- `marital`, `education` – basic demographics  
- `balance` – account balance  
- `contact`, `month`, `day`, `duration` – campaign contact info  
- `campaign`, `pdays`, `previous`, `poutcome` – campaign history  
- `deposit` – target variable (`yes`/`no`) indicating subscription

---

#### 5. Pipeline Details (Airflow DAG: `daily_pipeline`)

The DAG runs a **daily batch job** with 4 main tasks:

1. **`extract`**
   - Reads raw banking data from a SQLite database.
   - Adds ingestion timestamp: `ingest_ts`.
   - Logs number of rows and columns.

2. **`clean`**
   - Ensures `balance` is numeric.
   - Drops rows with null or negative `balance`.
   - Removes exact duplicates.
   - Basic data-quality safeguard: raises an error if no rows remain after cleaning.

3. **`aggregate`**  
   Produces two analytics DataFrames:

   - **`daily_summary`**
     - `dt` – date (normalized from the execution date)
     - `run_id` – unique identifier for the pipeline run
     - `total_customers`
     - `bal_sum`, `bal_mean`, `bal_min`, `bal_max`
     - `deposit_yes_count` – number of customers who subscribed (`deposit = "yes"`)
     - `deposit_conversion_rate` – `% of customers who subscribed`

   - **`job_campaign_summary`**
     - `job` – job type
     - `customer_count` – number of customers for that job
     - `avg_balance`, `total_balance`
     - `deposit_yes_count` – number of “yes” responses
     - `deposit_conversion_rate` – conversion rate per job
     - `dt`, `run_id` – for traceability

4. **`save`**
   - Writes both tables into SQLite:
     - `daily_summary`
     - `job_campaign_summary`
   - Exports full tables to CSV:
     - `data/daily_summary.csv`
     - `data/job_campaign_summary.csv`

---

#### 6. Example Output (Daily Summary)

Example row from `daily_summary.csv`:

| dt                         | total_customers | bal_sum  | bal_mean  | bal_min | bal_max | deposit_yes_count | deposit_conversion_rate |
|---------------------------|-----------------|----------|-----------|---------|---------|--------------------|-------------------------|
| 2025-11-29 00:00:00+00:00 | 10474           | 17284326 | 1650.21   | 0       | 81204   | *N*                | *X.X*                   |

This gives management a quick daily snapshot of customer volumes, balances, and campaign performance.

---

#### 7. Planned Dashboard (Power BI)

The exported CSVs are designed for use in a Power BI dashboard with visuals such as:

- **Daily KPIs**
  - Line chart: `dt` vs `deposit_conversion_rate`
  - KPI cards: `total_customers`, `bal_sum`, `bal_mean`

- **Job-based Performance**
  - Bar chart: `job` vs `deposit_conversion_rate`
  - Bar chart: `job` vs `customer_count`
  - Table: job, avg balance, total balance, conversion rate

Screenshots from Power BI can be added to the repo in a later iteration.

---

#### 8. How to Run (High Level)

1. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up Airflow (minimal local setup):

   ```bash
   export AIRFLOW_HOME=/path/to/airflow_home
   airflow db init
   ```

3. Copy `daily_pipeline.py` into the Airflow `dags/` folder.

4. Start Airflow:

   ```bash
   airflow webserver -p 8080
   airflow scheduler
   ```

5. Trigger the `daily_pipeline` DAG from the Airflow UI.

6. Check outputs in:
   - Database tables: `daily_summary`, `job_campaign_summary`
   - CSV files under `data/`.

---

#### 9. Author

- **Name:** Bhawish Raj  
- **Background:** Banking Analytics + Data Science / AI / ML  
- **Focus:** Building practical, end-to-end data projects for financial services.
