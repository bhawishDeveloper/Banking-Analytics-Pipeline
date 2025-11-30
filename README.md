# Banking-Analytics-Pipeline
End‑to‑End Banking Analytics Pipeline

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

High-level flow:

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

