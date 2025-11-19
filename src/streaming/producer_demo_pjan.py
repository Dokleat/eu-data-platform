from pathlib import Path
import json
import pandas as pd
from kafka import KafkaProducer

TOPIC_NAME = "demo_pjan_population"
KAFKA_BOOTSTRAP = "localhost:9092"


def get_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    return producer


def load_dataframe() -> pd.DataFrame:
    parquet_file = Path("data/lake/silver/demo_pjan/demo_pjan.parquet")
    if not parquet_file.exists():
        raise FileNotFoundError(f"Nuk u gjet file-i {parquet_file}")

    print(f"Lexoj Parquet nga: {parquet_file}")
    df = pd.read_parquet(parquet_file)

    print("Kolonat origjinale:", df.columns.tolist())

    # Rinemojmë kolonat në emra më të thjeshtë
    df = df.rename(
        columns={
            "Time": "time",
            "Geopolitical entity (reporting)": "geo",
            "Unit of measure": "unit",
            "Age class": "age",
            "Sex": "sex",
        }
    )

    print("Kolonat pas rename:", df.columns.tolist())

    # Sigurohemi që 'time' të jetë int (vit)
    try:
        df["time"] = df["time"].astype(int)
    except ValueError:
        df["time"] = df["time"].astype(str).str[:4].astype(int)

    return df


def send_messages(df: pd.DataFrame):
    producer = get_producer()

    total = len(df)
    print(f"Do dërgoj {total} mesazhe në topic '{TOPIC_NAME}'.")

    sent = 0

    for _, row in df.iterrows():
        # key: geo_year për idempotencë / grouping
        geo = row["geo"]
        year = int(row["time"])

        key = f"{geo}_{year}"

        event = {
            "geo": geo,
            "time": year,
            "measure_value": float(row["measure_value"]),
            "age": row.get("age"),
            "sex": row.get("sex"),
            "unit": row.get("unit"),
        }

        producer.send(TOPIC_NAME, key=key, value=event)
        sent += 1

        if sent % 1000 == 0:
            print(f"Dërguar {sent}/{total} mesazhe...")

    producer.flush()
    print(f"U dërguan gjithsej {sent} mesazhe në Kafka topic '{TOPIC_NAME}'.")


def main():
    df = load_dataframe()
    send_messages(df)


if __name__ == "__main__":
    main()