"""
Load raw CSVs into DuckDB — simulates loading into a data warehouse (Redshift/BigQuery)
In production this would be replaced by Fivetran / Airbyte / custom ingestion
"""

import duckdb
import os

DB_PATH  = os.path.join(os.path.dirname(__file__), "../data/banking.duckdb")
RAW_DIR  = os.path.join(os.path.dirname(__file__), "../data/raw")

TABLES = ["users", "transactions", "app_events", "cards"]

def load():
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for table in TABLES:
        path = f"{RAW_DIR}/{table}.csv"
        con.execute(f"""
            CREATE OR REPLACE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{path}', header=true)
        """)
        count = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        print(f"  Loaded raw.{table}: {count:,} rows")

    con.close()
    print(f"\nDuckDB saved to: {DB_PATH}")

if __name__ == "__main__":
    print("Loading data into DuckDB...")
    load()
