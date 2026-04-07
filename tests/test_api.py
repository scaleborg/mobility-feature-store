import os

import pytest

PARQUET_PATH = "data/offline/station_demand_15m/part-0.parquet"

if not os.path.exists(PARQUET_PATH):
    pytest.skip(
        f"Materialized data not found at {PARQUET_PATH}. "
        "Run materialization first.",
        allow_module_level=True,
    )

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_list_feature_views():
    resp = client.get("/feature-views")
    assert resp.status_code == 200
    data = resp.json()
    names = [fv["name"] for fv in data["feature_views"]]
    assert "station_demand_15m" in names


def test_get_feature_view():
    resp = client.get("/feature-views/station_demand_15m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "station_demand_15m"
    assert data["version"] == "v1"
    assert data["owner"] == "mobility-ml"
    assert "feature_schema" in data
    assert len(data["feature_schema"]) == 6


def test_get_feature_view_not_found():
    resp = client.get("/feature-views/nonexistent")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


def test_latest_retrieval():
    resp = client.post("/features/latest", json={
        "feature_views": ["station_demand_15m"],
        "entities": [{"station_id": "station_001"}],
    })
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 1
    assert results[0]["station_id"] == "station_001"
    assert results[0]["rides_started_lag_1"] is not None


def test_latest_missing_entity():
    resp = client.post("/features/latest", json={
        "feature_views": ["station_demand_15m"],
        "entities": [{"station_id": "station_999"}],
    })
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 1
    assert results[0]["rides_started_lag_1"] is None


def test_training_set_retrieval():
    resp = client.post("/training-set/retrieve", json={
        "feature_views": ["station_demand_15m"],
        "entities": [
            {"station_id": "station_001", "ts_15m": "2026-01-01T01:00:00"},
        ],
    })
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 1
    assert results[0]["station_id"] == "station_001"
    assert results[0]["rides_started_lag_1"] is not None


def test_training_set_before_history():
    resp = client.post("/training-set/retrieve", json={
        "feature_views": ["station_demand_15m"],
        "entities": [
            {"station_id": "station_001", "ts_15m": "2025-12-31T23:45:00"},
        ],
    })
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 1
    assert results[0]["rides_started_lag_1"] is None
