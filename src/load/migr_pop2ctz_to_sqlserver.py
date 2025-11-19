from pathlib import Path
import os

import pandas as pd
import pyodbc

# Debug: shfaq driver-at që sheh pyodbc
print("Driver-at e disponueshëm nga pyodbc:", pyodbc.drivers())

# A jemi duke xhiruar brenda Docker (Airflow container)?
RUNNING_IN_DOCKER = os.getenv("RUNNING_IN_DOCKER", "false").lower() == "true"
print("RUNNING_IN_DOCKER:", RUNNING_IN_DOCKER)

# Lexoj host-in për SQL Server nga env
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "localhost")

# Nëse jemi në Docker dhe host-i është ende "localhost" → përdor emrin e service-it në docker-compose: "sqlserver"
if RUNNING_IN_DOCKER and SQLSERVER_HOST == "localhost":
    SQLSERVER_HOST = "sqlserver"

print("SQLSERVER_HOST që po përdoret:", SQLSERVER_HOST)

SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQLSERVER_HOST},1433;"
    "DATABASE=EUDWH;"
    "UID=sa;"
    "PWD=Dokleat123.;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

print("SQLSERVER_CONN_STR:")
print(SQLSERVER_CONN_STR)


def get_connection():
    print("Po lidhem me SQL Server me connection string...")
    conn = pyodbc.connect(SQLSERVER_CONN_STR, timeout=5)
    print("Lidhja OK.")
    return conn


def ensure_dim_time(conn, years):
    cursor = conn.cursor()
    for y in years:
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dwh.dim_time WHERE year = ?)
            INSERT INTO dwh.dim_time (year) VALUES (?);
            """,
            int(y),
            int(y),
        )
    conn.commit()


def ensure_dim_country(conn, geos):
    cursor = conn.cursor()
    for g in geos:
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dwh.dim_country WHERE geo_code = ?)
            INSERT INTO dwh.dim_country (geo_code, country_name)
            VALUES (?, ?);
            """,
            g,
            g,
            g,
        )
    conn.commit()


def get_time_key(conn, year):
    cursor = conn.cursor()
    cursor.execute("SELECT time_key FROM dwh.dim_time WHERE year = ?", int(year))
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Nuk u gjet time_key për vitin {year}")
    return row[0]


def get_country_key(conn, geo_code):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT country_key FROM dwh.dim_country WHERE geo_code = ?", geo_code
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"Nuk u gjet country_key për geo_code {geo_code}")
    return row[0]


def load_fact_migration(conn, df: pd.DataFrame):
    cursor = conn.cursor()
    count = 0

    for _, row in df.iterrows():
        year = int(row["time"])
        geo = row["geo"]
        citizenship = row["citizenship"]
        value = int(row["measure_value"])
        unit = row["unit"]

        time_key = get_time_key(conn, year)
        country_key = get_country_key(conn, geo)

        cursor.execute(
            """
            INSERT INTO dwh.fact_migration (
                country_key,
                time_key,
                citizenship,
                value,
                unit,
                source_dataset
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            country_key,
            time_key,
            citizenship,
            value,
            unit,
            "MIGR_POP2CTZ",
        )
        count += 1

    conn.commit()
    print(f"U futën {count} rreshta në dwh.fact_migration.")


def main():
    parquet_file = Path("data/lake/silver/migr_pop2ctz/migr_pop2ctz.parquet")
    if not parquet_file.exists():
        raise FileNotFoundError(f"Nuk u gjet file-i {parquet_file}")

    print(f"Lexoj Parquet nga: {parquet_file}")
    df = pd.read_parquet(parquet_file)

    print("Kolonat origjinale në DataFrame:", df.columns.tolist())

    # Filtro Total / Number (ngjashëm si DEMO_PJAN)
    if "sex" in df.columns:
        df = df[df["sex"] == "Total"]
    if "age" in df.columns:
        df = df[df["age"] == "Total"]
    if "unit" in df.columns:
        df = df[df["unit"] == "Number"]

    # Rinemojmë kolonën e shtetësisë për thjeshtësi
    if "Country of citizenship" in df.columns:
        df = df.rename(columns={"Country of citizenship": "citizenship"})

    # Mund të kemi edhe kolonë "Time" → e kthejmë në "time" nëse ekziston
    if "Time" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"Time": "time"})

    print("Kolonat pas rename:", df.columns.tolist())

    expected_cols = ["time", "geo", "citizenship", "measure_value", "unit"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Mungojnë kolonat e pritura: {missing}")

    df = df[expected_cols]

    # Normalizojmë vitin
    try:
        df["time"] = df["time"].astype(int)
    except ValueError:
        df["time"] = df["time"].astype(str).str[:4].astype(int)

    print("Shembull rreshtash pas transformimit:")
    print(df.head())

    conn = get_connection()
    try:
        years = sorted(df["time"].unique().tolist())
        geos = sorted(df["geo"].unique().tolist())

        print("Vitet unike:", years)
        print("Vendet (geo) unike:", geos)

        ensure_dim_time(conn, years)
        ensure_dim_country(conn, geos)

        load_fact_migration(conn, df)
    finally:
        conn.close()
        print("Lidhja me SQL Server u mbyll.")


if __name__ == "__main__":
    main()