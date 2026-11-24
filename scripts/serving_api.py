import json
import os
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Music Big Data Serving API", version="1.0.0")
SERVING_DIR = os.path.join(os.path.dirname(__file__), "serving_data")

def load_table(table_name: str):
    file_path = os.path.join(SERVING_DIR, f"{table_name}.json")
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"Tabel '{table_name}' belum tersedia. Jalankan analytics_batch.py terlebih dahulu."
        )
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/")
def health_check():
    return {"status": "online", "layer": "Layer 4 - Serving API"}

@app.get("/api/descriptive/top-songs")
def get_top_songs():
    return load_table("serving_top_songs")

@app.get("/api/descriptive/genres")
def get_genre_stats():
    return load_table("serving_genre_stats")

@app.get("/api/diagnostic/audio")
def get_diagnostic_audio():
    return load_table("serving_diagnostic_audio")

@app.get("/api/diagnostic/correlations")
def get_correlations():
    return load_table("serving_correlations")

@app.get("/api/predictive/metrics")
def get_model_metrics():
    return load_table("serving_model_metrics")

@app.get("/api/prescriptive/recommendations")
def get_prescriptive_actions():
    return load_table("serving_prescriptive_actions")