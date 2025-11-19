from pathlib import Path
import json

import pandas as pd
from pyjstat import pyjstat


def get_latest_bronze_file(dataset_code: str) -> Path:
    """Gjen file-in më të fundit JSON nga bronze layer."""
    bronze_dir = Path("data/lake/bronze") / dataset_code.lower()
    files = sorted(bronze_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"Nuk u gjet asnjë file në {bronze_dir}")
    return files[-1]


def jsonstat_to_dataframe(json_path: Path) -> pd.DataFrame:
    """Lexon JSON-stat (me pyjstat) dhe kthen DataFrame."""
    print(f"Lexoj JSON-stat nga: {json_path}")

    # 1) Lexojmë file-in si dict
    with json_path.open("r", encoding="utf-8") as f:
        json_data = json.load(f)

    # 2) Konvertojmë JSON-stat → DataFrame me pyjstat
    #    from_json_stat kthen listë DataFrame-sh nëse ka disa dataset-e
    result = pyjstat.from_json_stat(json_data)
    if isinstance(result, list):
        df = result[0]
    else:
        df = result

    print("Kolonat origjinale:", df.columns.tolist())

    # 3) Riemërto kolonat në emra më të thjeshtë nëse ekzistojnë
    rename_map = {
        "Time": "time",
        "Geopolitical entity (reporting)": "geo",
        "Unit of measure": "unit",
        "Age class": "age",
        "Sex": "sex",
        "Citizenship": "citizenship",  # nëse ekziston
    }

    actual_rename = {old: new for old, new in rename_map.items() if old in df.columns}
    df = df.rename(columns=actual_rename)

    # 4) Vlera kryesore
    if "value" in df.columns:
        df = df.rename(columns={"value": "measure_value"})

    print("Kolonat pas rename:", df.columns.tolist())
    return df


def save_silver_parquet(dataset_code: str, df: pd.DataFrame) -> Path:
    """Ruaj DataFrame si Parquet në silver layer."""
    silver_dir = Path("data/lake/silver") / dataset_code.lower()
    silver_dir.mkdir(parents=True, exist_ok=True)
    out_file = silver_dir / f"{dataset_code.lower()}.parquet"

    df.to_parquet(out_file, index=False)
    print(f"U ruajt silver Parquet: {out_file}")
    return out_file


def main():
    dataset_code = "MIGR_POP2CTZ"
    latest_file = get_latest_bronze_file(dataset_code)
    df = jsonstat_to_dataframe(latest_file)
    save_silver_parquet(dataset_code, df)


if __name__ == "__main__":
    main()