# 🏦 Banking Analytics Pipeline

End-to-end analytics engineering project simulating a neobank data stack (N26-style).
Covers the full lifecycle: raw data → dbt transformations → tests → BI dashboard → CI/CD.

## Dashboard Screenshots

![KPI & Retention](docs/screenshots/dashboard_kpi_retention.png)
![Funnel & Segments](docs/screenshots/dashboard_funnel_segments.png)
![Churn & Volume](docs/screenshots/dashboard_churn_volume.png)

## Architecture
## Tech Stack

| Layer | Tool | Production equivalent |
|---|---|---|
| Data Warehouse | DuckDB | AWS Redshift |
| Transformations | dbt Core | dbt Cloud |
| Orchestration | GitHub Actions | Airflow |
| BI / Dashboards | Streamlit + Plotly | Metabase / Tableau |
| Containerization | Docker | ECS / Kubernetes |
| CI/CD | GitHub Actions | GitHub Actions |

## Key Metrics Modelled

- **D1 / D7 / D30 Cohort Retention** by signup month, country, plan
- **Activation Funnel**: Registration → KYC → Card Activation → First Transfer
- **Monthly Churn Rate** by plan type and country
- **User Engagement Segments**: dormant / low / medium / high
- **ARPU & LTV** per user
- **Monthly Transaction Volume**

## Quickstart

```bash
# 1. Clone and install
git clone https://github.com/YOUR_USERNAME/banking-analytics-pipeline
cd banking-analytics-pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Generate data + load into DuckDB
python scripts/generate_data.py
python scripts/load_to_duckdb.py

# 3. Run dbt models and tests
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..

# 4. Launch dashboard
python -m streamlit run streamlit_app/app.py
```

## Project Structure
## Data Model

**5,000 users** · **~125,000 transactions** · **~90,000 app events** · **~5,800 cards**

Simulated over 2 years (2023–2024) with realistic distributions:
- 65% free / 25% smart / 10% metal plan split
- 88% KYC verification rate
- Country mix: DE (40%), AT (12%), ES (12%), FR (10%), others
