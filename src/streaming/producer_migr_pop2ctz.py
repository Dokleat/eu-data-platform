import json
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

TOPIC_NAME = "migr_population_stream"
KAFKA_BOOTSTRAP = "localhost:9092"

def get_dataframe():
    parquet_file = Path("data/lake/silver/migr_pop2ctz/migr_pop2ctz.parquet")
    if not parquet_file.exists():
        raise FileNotFoundError(f"{parquet_file} not found")
    print(f"Reading Parquet: {parquet_file}") 
    return pd.read_parquet(parquet_file)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None
    )

def send_messages(df):
    producer = create_producer()
    total = len(df)
    print(f"Sending {total} messages...")

    for idx, row in df.iterrows():
        key = f"{row['geo']}_{row['time']}"
        payload = row.to_dict()

        producer.send(TOPIC_NAME, key=key, value=payload)

        if idx % 200 == 0:
            print(f"Sent {idx}/{total}")

    producer.flush()
    print("Done sending.")
    producer.close()

if __name__ == "__main__":
    df = get_dataframe()
    send_messages(df)
