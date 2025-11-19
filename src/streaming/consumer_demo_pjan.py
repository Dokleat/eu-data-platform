import json
import math
import pyodbc
from kafka import KafkaConsumer

TOPIC_NAME = "demo_pjan_population"
KAFKA_BOOTSTRAP = "localhost:9092"

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
    print("Po lidhem me SQL Server nga consumer_demo_pjan...")
    conn = pyodbc.connect(SQLSERVER_CONN_STR, timeout=5)
    print("Lidhja me SQL Server OK.")
    return conn


def insert_event(conn, event: dict):
    cursor = conn.cursor()

    geo = event["geo"]
    year = int(event["time"])

    raw_value = event["measure_value"]

    # Skip nëse vlera është None ose NaN
    if raw_value is None or (isinstance(raw_value, float) and math.isnan(raw_value)):
        print(f"[SKIP] Event me measure_value NaN/None për geo={geo}, year={year}")
        return

    value = int(raw_value)

    age = event.get("age")
    sex = event.get("sex")
    unit = event.get("unit")

    cursor.execute(
        """
        INSERT INTO dwh.fact_population_stream
            (geo_code, year, population_value, age, sex, unit)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        geo,
        year,
        value,
        age,
        sex,
        unit,
    )
    conn.commit()


def consume_messages():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="demo_pjan_consumer_group_v2",
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
    consume_messages()