import pandas as pd
import psycopg2
import os
from glob import glob

PARQUET_PATH = "/opt/airflow/data/parquet/*.parquet"

DB_PARAMS = {
    "host": "postgres",
    "port": 5432,
    "database": "ecommerce",
    "user": "airflow",
    "password": "airflow"
}

def load_parquet_to_postgres():
    files = glob(PARQUET_PATH)
    if not files:
        print("No Parquet files found.")
        return

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for file in files:
        df = pd.read_parquet(file)
        print(f"Inserting {len(df)} records from {file}")
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO user_events (user_id, event_type, event_time, product_id, price)
                VALUES (%s, %s, %s, %s, %s)
            """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    print("Data inserted successfully.")

if __name__ == "__main__":
    load_parquet_to_postgres()
