# EU Population & Migration Data Platform (Eurostat · Kafka · Azurite · SQL Server · Airflow · Metabase · FastAPI)

This repository contains an end-to-end data platform built around **Eurostat** public datasets for EU population and migration.  
The goal is to simulate a realistic, production-style **Data Engineering + ML** project that can be used for learning, portfolio, and interviews.
## Technologies

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20.svg)](https://kafka.apache.org/)
[![Azulite](https://img.shields.io/badge/Azurite-Blob%20Storage%20Emulator-0078D4.svg)](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-Data%20Warehouse-CC2927.svg)](https://www.microsoft.com/sql-server/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE.svg)](https://airflow.apache.org/)
[![Metabase](https://img.shields.io/badge/Metabase-BI%20Dashboards-509EE3.svg)](https://www.metabase.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-ML%20API-009688.svg)](https://fastapi.tiangolo.com/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-ML%20Model-F7931E.svg)](https://scikit-learn.org/)
[![Docker](https://img.shields.io/badge/Docker-Containers-2496ED.svg)](https://www.docker.com/)
[![JSON-stat](https://img.shields.io/badge/JSON--stat-Data%20Parsing-000000.svg)](https://json-stat.org/)
[![Eurostat API](https://img.shields.io/badge/Eurostat-Open%20Data-003399.svg)](https://ec.europa.eu/eurostat/)

The platform implements a full lifecycle:

- **Ingest** Eurostat data (JSON-stat API) with Python
- Land data into a **local Data Lake** (bronze / silver)
- Stream records through **Apache Kafka**
- Store curated data in a **SQL Server Data Warehouse**
- Orchestrate ETL and ML pipelines with **Apache Airflow**
- Expose analytics via **Metabase** dashboards
- Train and serve an **ML model** (population forecasting) via **FastAPI**

---

## 1. Project Goals

This project is designed to:

- Show a realistic **modern data stack** on a local/dev environment
- Cover both **batch** and **streaming** patterns
- Demonstrate a clean **layered architecture** (bronze → silver → DW → BI/ML)
- Include an **ML component** that is fully integrated with the pipeline
- Be easy to run on a laptop using Docker and Python

It’s intentionally over-engineered for a single domain (population & migration) to highlight tools, patterns, and best practices.

---

## 2. High-Level Architecture

The platform follows the classic pattern:

> **Collect → Ingest → Store → Compute → Consume**

### 2.1 Data Sources

- **Eurostat Statistics API (JSON-stat):**
  - `DEMO_PJAN` – population on 1 January by country, year, etc.
  - `MIGR_POP2CTZ` – population by citizenship / migration-related measures

### 2.2 Components

- **Ingestion & Transformation:** Python (requests, pandas, jsonstat, pyarrow)
- **Data Lake:** Local file system with `bronze` (raw JSON) and `silver` (Parquet)
- **Object Storage Emulator:** Azurite (Azure Blob Storage emulator) – optional
- **Streaming:** Apache Kafka (Confluent image)
- **Data Warehouse:** SQL Server 2022 (Docker)
- **Orchestrator:** Apache Airflow 2.x
- **BI:** Metabase
- **ML:** scikit-learn + joblib
- **ML API:** FastAPI + Uvicorn

### 2.3 Logical Flow

1. **Collect / Ingest**
   - Python ingestion scripts call Eurostat APIs.
   - The raw JSON-stat payloads are stored in the Data Lake (bronze).
   - Optionally, bronze files are uploaded into Azurite for blob-based storage.

2. **Store – Data Lake**
   - Bronze: raw JSON (JSON-stat responses)
   - Silver: cleaned, tabular **Parquet** files after JSON-stat parsing

3. **Store – Data Warehouse**
   - SQL Server database `EUDWH` with:
     - `dwh.dim_country` – country dimension (GEO codes)
     - `dwh.dim_time` – time dimension (year)
     - `dwh.fact_population` – fact table for population (batch)
     - `dwh.fact_population_stream` – streaming sink for population events
     - `dwh.fact_migration_stream` – streaming sink for migration events

4. **Streaming**
   - Kafka topic (e.g. `demo_pjan_population`) receives events from Parquet.
   - A Kafka consumer writes events into SQL Server streaming tables.
   - This simulates a real-time ingestion layer on top of the batch lake.

5. **Compute**
   - **Batch ETL** is orchestrated through Airflow DAGs:
     - `ingest` (Eurostat → bronze)
     - `transform` (bronze → silver)
     - `load` (silver → Data Warehouse)
   - **ML Retraining** is also orchestrated as an Airflow DAG that:
     - pulls data from the DW
     - retrains the population model
     - saves a versioned model artifact

6. **Consume**
   - **Metabase BI** connects to SQL Server and provides:
     - population trends per country
     - migration trends per country
     - population vs migration correlations
   - **Jupyter / notebooks** are used for EDA and ML experimentation.
   - **FastAPI** exposes an `/predict-population` endpoint for online scoring.

---

## 3. Data Model

### 3.1 Dimensions

- `dwh.dim_country`
  - `country_key` (PK)
  - `geo_code` (e.g. DE, FR, EU27_2020)
  - `country_name`

- `dwh.dim_time`
  - `time_key` (PK)
  - `year` (INT)

### 3.2 Fact Tables

- `dwh.fact_population`  
  Batch-loaded population measurements:

  - `population_id` (PK)
  - `country_key` (FK → dim_country)
  - `time_key` (FK → dim_time)
  - `population_value`
  - `source_dataset`
  - `load_ts`

- `dwh.fact_population_stream`  
  Streaming sink for population events from Kafka:

  - `population_stream_id` (PK)
  - `geo_code`
  - `year`
  - `population_value`
  - `event_ts` (ingestion timestamp)

- `dwh.fact_migration_stream`  
  Streaming sink for migration/citizenship metrics:

  - `migration_id` (PK)
  - `geo_code`
  - `year`
  - `value`
  - `citizenship`
  - `unit`
  - `event_ts`

This separation allows both **clean dimensional modeling** and **raw event capture** for later processing.

---

## 4. Pipelines

### 4.1 Batch ETL (Eurostat → Lake → DW)

For each dataset (e.g. `DEMO_PJAN`):

1. **Ingest**
   - `src/collect/ingest_demo_pjan.py`
   - Calls Eurostat API, writes timestamped JSON into `data/lake/bronze/demo_pjan`.

2. **Transform**
   - `src/transform/demo_pjan_raw_to_silver.py`
   - Parses JSON-stat using `jsonstat` and converts to a tabular pandas DataFrame.
   - Saves standardized Parquet into `data/lake/silver/demo_pjan/demo_pjan.parquet`.

3. **Load**
   - `src/load/demo_pjan_to_sqlserver.py`
   - Reads Parquet, ensures dimension members in `dim_country` and `dim_time`, and loads records into `fact_population`.

All three steps are orchestrated by the Airflow DAG `demo_pjan_pipeline`.

---

### 4.2 Streaming Pipeline (Parquet → Kafka → SQL Server)

1. **Producer**
   - `src/streaming/producer_demo_pjan.py`
   - Reads the silver Parquet file.
   - Serializes each row as JSON and publishes to Kafka topic `demo_pjan_population` (keyed by `geo` + `time`).

2. **Consumer**
   - `src/streaming/consumer_demo_pjan.py`
   - Subscribes to `demo_pjan_population`.
   - Inserts events into `dwh.fact_population_stream` in SQL Server.

A similar pattern can be applied to migration data (`MIGR_POP2CTZ`) and other Eurostat datasets.

---

## 5. Orchestration with Airflow

Airflow runs in its own docker-compose stack and mounts the project folder.  
The main DAGs include:

- `demo_pjan_pipeline.py`
  - `ingest_demo_pjan` → `transform_demo_pjan` → `load_demo_pjan_to_sqlserver`
  - Scheduled `@daily` (configurable)

- `de_population_model_retraining.py`
  - Single task: `train_and_save_de_population_model`
  - Runs `src/ml/train_de_population_model.py`
  - Scheduled `@monthly` (configurable)
  - Produces versioned model artifacts and updates the `latest` model file.

Airflow talks to SQL Server via `host.docker.internal` and uses `_PIP_ADDITIONAL_REQUIREMENTS` to install the same Python dependencies inside the containers.

---

## 6. BI & Analytics with Metabase

Metabase connects directly to the `EUDWH` SQL Server instance and can be used to:

- Explore dimensions and facts
- Build charts for:
  - population trends (line charts) per country
  - migration values per country (bar or line charts)
  - scatter plots of population vs migration
- Compose a **“EU Population & Migration”** dashboard combining multiple questions.

This makes the platform easy to demo in a browser without extra coding.

---

## 7. ML: Population Forecasting

The ML component focuses initially on **Germany (DE)** as a single-country example, with label:

- `population_value` per year

and features:

- `year`
- `total_migration` (aggregated migration value per year for DE)

### 7.1 Training

The script `src/ml/train_de_population_model.py`:

1. Reads population (`fact_population`) and migration (`fact_migration_stream`) from SQL Server.
2. Aggregates migration for DE by year.
3. Joins population and migration into a single DataFrame.
4. Uses a simple **LinearRegression** model from scikit-learn:
   - `X = [year, total_migration]`
   - `y = population_value`
5. Uses the last 2 years as a test set to compute **MAE** and **RMSE**.
6. Saves the model twice:
   - Versioned: `models/de_population_model_YYYYMMDDTHHMMSS.pkl`
   - Latest: `models/de_population_model.pkl`

### 7.2 Retraining

The Airflow DAG `de_population_model_retraining` calls this script on a schedule (e.g. monthly), allowing continuous retraining as new data arrives.

---

## 8. FastAPI – ML Serving

The FastAPI app `src/api/population_service.py` exposes the trained model as an HTTP service.

- Loads the latest model from `models/de_population_model.pkl`
- Exposes:

  - `GET /` – basic health/info endpoint
  - `POST /predict-population` – population forecast endpoint:

    **Request body:**

    ```json
    {
      "country": "DE",
      "year": 2030,
      "total_migration": 200000
    }
    ```

    **Response:**

    ```json
    {
      "country": "DE",
      "year": 2030,
      "total_migration": 200000.0,
      "population_prediction": 83200000
    }
    ```

> Note:  
> In this first version, the model is trained for **DE** only and will reject other country codes.  
> Extending to multiple countries is straightforward by adjusting the training script and API logic.

---

## 9. Local Development Setup (Short Version)

1. **Clone & create venv**

   ```bash
   git clone <this-repo>
   cd eu-data-platform
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt  # or follow the stack in this README
