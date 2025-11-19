import requests
import json
from pathlib import Path
from datetime import datetime

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

def build_url(dataset_code, params: dict) -> str:
    query_parts = []
    for key, value in params.items():
        if isinstance(value, list):
            for v in value:
                query_parts.append(f"{key}={v}")
        else:
            query_parts.append(f"{key}={value}")
    query = "&".join(query_parts)
    return f"{BASE_URL}/{dataset_code}?{query}"

def fetch_dataset(dataset_code: str, params: dict):
    url = build_url(dataset_code, params)
    print(f"Fetching from: {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def save_local_bronze(dataset_code: str, data: dict) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_dir = Path("data/lake/bronze") / dataset_code.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{dataset_code.lower()}_{ts}.json"

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"Saved bronze file: {out_file}")
    return out_file

if __name__ == "__main__":
    dataset_code = "MIGR_POP2CTZ"
    params = {
        "lang": "EN",
        "lastTimePeriod": 10,
        "geo": ["DE", "FR", "IT", "ES", "PL", "EU27_2020"]
    }

    data = fetch_dataset(dataset_code, params)
    save_local_bronze(dataset_code, data)
