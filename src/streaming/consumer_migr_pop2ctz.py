import json
import math
import time

import pyodbc
from kafka import KafkaConsumer

# Kafka
TOPIC_NAME = "migr_population_stream"
KAFKA_BOOTSTRAP = "localhost:9092"

# SQL Server – njësoj si tek DEMO_PJAN ku punoi
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=127.0.0.1,1433;"
    "DATABASE=EUDWH;"
    "UID=sa;"
    "PWD=Dokleat123.;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)


def get_sql_connection():
    print("Po lidhem me SQL Server nga consumer_migr_pop2ctz...")
    conn = pyodbc.connect(SQLSERVER_CONN_STR, timeout=5)
    print("Lidhja me SQL Server OK.")
    return conn


def insert_event(conn, event: dict):
    cursor = conn.cursor()

    geo = event.get("geo")
    year = int(event.get("time"))

    raw_value = event.get("measure_value")
    if raw_value is None or (isinstance(raw_value, float) and math.isnan(raw_value)):
        print(f"[SKIP] measure_value NaN/None për geo={geo}, year={year}")
        return

    value = int(raw_value)

    citizenship = event.get("citizen") or event.get("citizenship")
    unit = event.get("unit")

    cursor.execute(
        """
        INSERT INTO dwh.fact_migration_stream 
            (geo_code, year, value, citizenship, unit)
        VALUES (?, ?, ?, ?, ?);
        """,
        geo,
        year,
        value,
        citizenship,
        unit,
    )
    conn.commit()


def consume():
    group_id = f"migr_pop_consumer_{int(time.time())}"
    print(f"Përdor group_id: {group_id}")

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
    )

    conn = get_sql_connection()

    print(f"Consumer i lidhur te topic '{TOPIC_NAME}'. Duke pritur mesazhe...")

    try:
        for message in consumer:
            key = message.key
            event = message.value
            print(f"Marrë mesazh: key={key}, event={event}")
            insert_event(conn, event)
    finally:
        conn.close()
        consumer.close()


if __name__ == "__main__":
    consume()