import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyodbc
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib


# -----------------------
# Config
# -----------------------

SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=EUDWH;"
    "UID=sa;"
    "PWD=YourStrong!Passw0rd;"
    "Encrypt=no;"
)

MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)


# -----------------------
# Data loading
# -----------------------

def get_connection():
    return pyodbc.connect(SQLSERVER_CONN_STR)


def load_population():
    query_population = """
    SELECT
        c.geo_code,
        t.year,
        f.population_value
    FROM dwh.fact_population f
    JOIN dwh.dim_country c ON f.country_key = c.country_key
    JOIN dwh.dim_time t ON f.time_key = t.time_key
    WHERE c.geo_code = 'DE'
    ORDER BY t.year;
    """
    with get_connection() as conn:
        df = pd.read_sql(query_population, conn)
    return df


def load_migration():
    query_migration = """
    SELECT
        geo_code,
        year,
        value,
        citizenship
    FROM dwh.fact_migration_stream
    WHERE geo_code = 'DE'
    ORDER BY year;
    """
    with get_connection() as conn:
        df = pd.read_sql(query_migration, conn)
    return df


# -----------------------
# Training logic
# -----------------------

def train_model():
    df_pop = load_population()
    df_migr = load_migration()

    if df_pop.empty:
        raise RuntimeError("Nuk u gjetën të dhëna popullsie për DE.")
    if df_migr.empty:
        print("Paralajmërim: Nuk u gjetën të dhëna migracioni. Do përdorim vetëm vitin si feature.")

    # filtrimi për DE (edhe pse query e bën vetë)
    df_de = df_pop[df_pop["geo_code"] == "DE"].copy()

    # agregim i migracionit sipas vitit
    if not df_migr.empty:
        df_migr_de = df_migr[df_migr["geo_code"] == "DE"].copy()
        df_migr_agg = (
            df_migr_de
            .groupby("year", as_index=False)["value"]
            .sum()
            .rename(columns={"value": "total_migration"})
        )
        df_de_full = df_de.merge(df_migr_agg, on="year", how="left")
    else:
        df_de_full = df_de.copy()
        df_de_full["total_migration"] = 0.0

    # mbush NaN me 0 për total_migration
    df_de_full["total_migration"] = df_de_full["total_migration"].fillna(0.0)

    # features dhe target
    X = df_de_full[["year", "total_migration"]].values
    y = df_de_full["population_value"].values

    if len(X) < 4:
        raise RuntimeError("Shumë pak të dhëna për të trajnuar modelin (duhen të paktën 4 rreshta).")

    # ndarja train/test: 2 vitet e fundit për test
    X_train = X[:-2]
    y_train = y[:-2]
    X_test = X[-2:]
    y_test = y[-2:]

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred_test = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred_test)
    rmse = mean_squared_error(y_test, y_pred_test, squared=False)

    print("Rezultatet e modelit për DE (year + migration):")
    print("MAE:", mae)
    print("RMSE:", rmse)
    print("Test points (year, real, pred):")
    for row, real_v, pred_v in zip(X_test, y_test, y_pred_test):
        print(f"  year={int(row[0])}, real={int(real_v)}, pred={int(pred_v)}")

    return model


# -----------------------
# Save model with versioning
# -----------------------

def save_model(model):
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    # versionuar sipas timestamp-it
    versioned_path = MODELS_DIR / f"de_population_model_{timestamp}.pkl"
    latest_path = MODELS_DIR / "de_population_model.pkl"

    joblib.dump(model, versioned_path)
    print(f"Modeli u ruajt te: {versioned_path}")

    # gjithmonë update edhe 'latest'
    joblib.dump(model, latest_path)
    print(f"Modeli 'latest' u ruajt te: {latest_path}")


def main():
    model = train_model()
    save_model(model)


if __name__ == "__main__":
    main()
