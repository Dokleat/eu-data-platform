from pathlib import Path
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
    """Lexon JSON-stat dhe kthen DataFrame me pyjstat."""
    print(f"Lexoj JSON-stat nga: {json_path}")

    # Jepi direkt file handler-in pyjstat-it
    with json_path.open("r", encoding="utf-8") as f:
        dataset = pyjstat.Dataset.read(f)

    # Ktheje në pandas DataFrame
    df = dataset.write("dataframe")

    # Në shumë dataset-e kolona quhet 'value'
    if "value" in df.columns:
        df = df.rename(columns={"value": "measure_value"})

    print("Kolonat në DataFrame:", df.columns.tolist())
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
    dataset_code = "DEMO_PJAN"
    latest_file = get_latest_bronze_file(dataset_code)
    df = jsonstat_to_dataframe(latest_file)
    save_silver_parquet(dataset_code, df)


if __name__ == "__main__":
    main()