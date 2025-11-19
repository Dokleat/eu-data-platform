from pathlib import Path
from typing import Optional

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# -----------------------
# Config & model loading
# -----------------------

MODELS_DIR = Path("models")
MODEL_FILE = MODELS_DIR / "de_population_model.pkl"

if not MODEL_FILE.exists():
    raise RuntimeError(f"Model file not found: {MODEL_FILE}. Train and save the model first.")

model = joblib.load(MODEL_FILE)

app = FastAPI(
    title="EU Population Prediction API",
    description="API për parashikimin e popullsisë (aktualisht vetëm DE) bazuar në vit dhe migracion.",
    version="1.0.0",
)


# -----------------------
# Request & Response schemas
# -----------------------

class PopulationRequest(BaseModel):
    country: str = "DE"
    year: int
    total_migration: Optional[float] = None  # nëse mungon, do përdorim 0 ose një default


class PopulationResponse(BaseModel):
    country: str
    year: int
    total_migration: float
    population_prediction: int


# -----------------------
# Helper functions
# -----------------------

def build_feature_vector(req: PopulationRequest) -> np.ndarray:
    """
    Për momentin modeli është trajnuar vetëm për Gjermaninë (DE).
    Feature: [year, total_migration]
    """
    if req.country != "DE":
        # mund ta lejojmë, por duke paralajmëruar që modeli është gjerman-only
        # për tani hedhim error të qartë
        raise HTTPException(
            status_code=400,
            detail="Aktualisht modeli mbështet vetëm country='DE'."
        )

    # nëse total_migration mungon, përdorim 0 (ose mund ta ndryshosh me një mesatare të ruajtur diku)
    total_migration = req.total_migration if req.total_migration is not None else 0.0

    # model2 priste vektor [year, total_migration]
    return np.array([[req.year, total_migration]])


# -----------------------
# Routes
# -----------------------

@app.get("/")
def root():
    return {"message": "EU Population Prediction API. Përdor POST /predict-population."}


@app.post("/predict-population", response_model=PopulationResponse)
def predict_population(payload: PopulationRequest):
    features = build_feature_vector(payload)
    pred = model.predict(features)[0]

    return PopulationResponse(
        country=payload.country,
        year=payload.year,
        total_migration=float(payload.total_migration or 0.0),
        population_prediction=int(pred),
    )
