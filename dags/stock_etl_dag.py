import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from io import StringIO

# Environment variables
API_KEY = os.getenv("API_KEY")
AIVEN_DB_HOST = os.getenv("AIVEN_DB_HOST")
AIVEN_DB_PORT = os.getenv("AIVEN_DB_PORT")
AIVEN_DB_NAME = os.getenv("AIVEN_DB_NAME")
AIVEN_DB_USER = os.getenv("AIVEN_DB_USER")
AIVEN_DB_PASSWORD = os.getenv("AIVEN_DB_PASSWORD")
AIVEN_SSLMODE = os.getenv("AIVEN_SSLMODE", "require")
AIVEN_CA_CERT_PATH = os.getenv("AIVEN_CA_CERT_PATH")
STOCK_TICKERS = os.getenv("STOCK_TICKERS", "NVDA,TSLA,MSFT,GOOGL").split(",")

# Database connection
def get_db_url():
    return (
        f"postgresql+psycopg2://{AIVEN_DB_USER}:{AIVEN_DB_PASSWORD}"
        f"@{AIVEN_DB_HOST}:{AIVEN_DB_PORT}/{AIVEN_DB_NAME}"
        f"?sslmode={AIVEN_SSLMODE}&sslrootcert={AIVEN_CA_CERT_PATH}"
    )

# Extract (Fetch data from Polygon from 2025-10-17 → today)
def extract_data(**context):
    from_date = datetime(2025, 10, 17)
    to_date = datetime.utcnow()

    all_data = []
    for ticker in STOCK_TICKERS:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/min/"
            f"{from_date.strftime('%Y-%m-%d')}/{to_date.strftime('%Y-%m-%d')}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={API_KEY}"
        )
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if "results" in data and data["results"]:
                for item in data["results"]:
                    ts = datetime.utcfromtimestamp(item["t"] / 1000)
                    all_data.append({
                        "ticker": ticker,
                        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                        "open": item["o"],
                        "high": item["h"],
                        "low": item["l"],
                        "close": item["c"],
                        "volume": item["v"]
                    })
        else:
            print(f"Failed to fetch data for {ticker}: {response.text}")

    print(f"Fetched {len(all_data)} rows from {from_date} to {to_date}")
    context['ti'].xcom_push(key='raw_data', value=json.dumps(all_data))

# Transform (Clean & Deduplicate)
def transform_data(**context):
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract_task')
    data = json.loads(raw_data)
    df = pd.DataFrame(data)

    if df.empty:
        print("No data fetched.")
        return

    df.dropna(inplace=True)
    # Drop duplicates within this batch only (not DB)
    df.drop_duplicates(subset=["ticker", "timestamp"], inplace=True)
    context['ti'].xcom_push(key='transformed_data', value=df.to_json(orient="records"))

# Load (Append all new data to DB, no overwrites)
def load_data(**context):
    transformed_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_task')
    if not transformed_json:
        print("No transformed data to load.")
        return

    df = pd.read_json(StringIO(transformed_json))
    engine = create_engine(get_db_url())

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10),
                timestamp TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT
            );
        """))

        # Append-only load: do not skip or replace
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO stock_data (ticker, timestamp, open, high, low, close, volume)
                VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume)
            """), {
                "ticker": row["ticker"],
                "timestamp": row["timestamp"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"]
            })

    print(f"Loaded {len(df)} rows into stock_data table.")

# DAG Definition
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='stock_etl',
    default_args=default_args,
    description='Live-growing Stock Market ETL Pipeline',
    schedule_interval='@hourly',   # change to @daily if you prefer
    start_date=days_ago(1),
    catchup=False,
    tags=['stock', 'etl', 'polygon']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
