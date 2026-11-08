"""ETL pipeline extracting daily currency exchange rates into MongoDB."""

from datetime import datetime, timedelta, timezone
import logging
import os
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator

API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
XCOM_RAW_DATA = "raw_exchange_data"
XCOM_TRANSFORMED_DATA = "transformed_exchange_data"

MONGO_DEFAULT_URI = "mongodb://admin:adminpassword@mongodb:27017/?authSource=admin"
MONGO_DEFAULT_DB = "currency_db"
MONGO_DEFAULT_COLLECTION = "exchange_rates"


def extract_currency_rates(**kwargs):
    """Fetch rates from Frankfurter API and store raw payload in Named XCom."""
    ti = kwargs["ti"]
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    raw_data = response.json()

    if not isinstance(raw_data, list) or not raw_data:
        raise ValueError(f"Expected non-empty list from API, received: {type(raw_data).__name__}")

    ti.xcom_push(key=XCOM_RAW_DATA, value=raw_data)
    logging.info("Extracted %d rate records from Frankfurter API.", len(raw_data))


def transform_exchange_rates(**kwargs):
    """Standardize exchange rate quotes into target schema and push to Named XCom."""
    ti = kwargs["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_currency_rates", key=XCOM_RAW_DATA)

    if not raw_data:
        raise ValueError("XCom key '%s' returned empty or missing data." % XCOM_RAW_DATA)

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed = []

    for item in raw_data:
        transformed.append({
            "base_currency": str(item["base"]),
            "target_currency": str(item["quote"]),
            "exchange_rate": float(item["rate"]),
            "rate_date": str(item["date"]),
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at,
        })

    ti.xcom_push(key=XCOM_TRANSFORMED_DATA, value=transformed)
    logging.info("Transformed %d records to target schema.", len(transformed))


def load_to_mongodb(**kwargs):
    """Upsert standardized records into MongoDB currency_db.exchange_rates collection."""
    ti = kwargs["ti"]
    records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED_DATA)

    if not records:
        raise ValueError("XCom key '%s' returned empty or missing data." % XCOM_TRANSFORMED_DATA)

    mongo_uri = os.getenv("MONGO_URI", MONGO_DEFAULT_URI)
    db_name = os.getenv("MONGO_DATABASE", MONGO_DEFAULT_DB)
    coll_name = os.getenv("MONGO_COLLECTION", MONGO_DEFAULT_COLLECTION)

    with MongoClient(mongo_uri) as client:
        collection = client[db_name][coll_name]
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
        )

        for record in records:
            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"],
                },
                {"$set": record},
                upsert=True,
            )

    logging.info("Upserted %d records into MongoDB %s.%s", len(records), db_name, coll_name)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="currency_exchange_etl",
    default_args=default_args,
    description="Daily ETL pipeline extracting Frankfurter API currency rates into MongoDB",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["etl", "currency", "mongodb", "frankfurter"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates,
    )

    task_transform = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
    )

    task_load = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    task_extract >> task_transform >> task_load
