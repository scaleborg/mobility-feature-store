import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pytest

PARQUET_PATH = "data/offline/station_demand_15m/part-0.parquet"

if not os.path.exists(PARQUET_PATH):
    pytest.skip(
        f"Materialized data not found at {PARQUET_PATH}. "
        "Run: PYTHONPATH=. python3 scripts/materialize.py --feature-view station_demand_15m",
        allow_module_level=True,
    )

from scripts.retrieve import retrieve_features

FEATURE_COLS = [
    "rides_started_lag_1",
    "rides_started_lag_4",
    "rides_started_lag_96",
    "rides_started_rolling_mean_4",
    "rides_started_rolling_mean_96",
    "rides_ended_lag_1",
]


@pytest.fixture
def feature_parquet():
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").df()
    conn.close()
    return df


def test_no_leakage(feature_parquet):
    entity_df = pd.DataFrame({
        "station_id": ["station_001", "station_001", "station_003", "station_003"],
        "ts_15m": [
            datetime(2026, 1, 1, 1, 0),
            datetime(2026, 1, 3, 12, 30),
            datetime(2026, 1, 1, 0, 7),
            datetime(2026, 1, 5, 8, 0),
        ],
    })

    conn = duckdb.connect()
    conn.register("entity_df", entity_df)
    conn.execute(f"CREATE VIEW features AS SELECT * FROM read_parquet('{PARQUET_PATH}')")

    diagnostic = conn.execute("""
        SELECT e.station_id, e.ts_15m AS entity_ts, f.ts_15m AS feature_ts
        FROM entity_df e
        ASOF LEFT JOIN features f
            ON e.station_id = f.station_id
            AND e.ts_15m >= f.ts_15m
    """).df()
    conn.close()

    for _, row in diagnostic.iterrows():
        if pd.notna(row["feature_ts"]):
            assert row["feature_ts"] <= row["entity_ts"], (
                f"leakage: feature_ts={row['feature_ts']} > entity_ts={row['entity_ts']}"
            )


def test_correct_lag_match(feature_parquet):
    target_ts = datetime(2026, 1, 1, 2, 0)
    entity_df = pd.DataFrame({
        "station_id": ["station_001"],
        "ts_15m": [target_ts],
    })

    result = retrieve_features(entity_df, ["station_demand_15m"])

    expected_row = feature_parquet[
        (feature_parquet["station_id"] == "station_001")
        & (feature_parquet["ts_15m"] <= target_ts)
    ].iloc[-1]

    for col in FEATURE_COLS:
        expected = expected_row[col]
        actual = result.iloc[0][col]
        if pd.isna(expected):
            assert pd.isna(actual), f"{col}: expected NaN, got {actual}"
        else:
            assert abs(actual - expected) < 1e-9, f"{col}: expected {expected}, got {actual}"


def test_row_count_preserved():
    entity_df = pd.DataFrame({
        "station_id": ["station_001"] * 5 + ["station_002"] * 3,
        "ts_15m": [
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 0, 15),
            datetime(2026, 1, 1, 0, 30),
            datetime(2026, 1, 1, 0, 45),
            datetime(2026, 1, 2, 0, 0),
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 3, 6, 0),
            datetime(2026, 1, 7, 23, 45),
        ],
    })

    result = retrieve_features(entity_df, ["station_demand_15m"])
    assert len(result) == len(entity_df)


def test_entity_before_first_feature():
    entity_df = pd.DataFrame({
        "station_id": ["station_001"],
        "ts_15m": [datetime(2025, 12, 31, 23, 45)],
    })

    result = retrieve_features(entity_df, ["station_demand_15m"])

    assert len(result) == 1
    for col in FEATURE_COLS:
        assert pd.isna(result.iloc[0][col]), f"{col} should be NULL for pre-history entity"


def test_extra_entity_columns_preserved():
    entity_df = pd.DataFrame({
        "station_id": ["station_001", "station_002"],
        "ts_15m": [datetime(2026, 1, 2, 0, 0), datetime(2026, 1, 2, 0, 0)],
        "label": [1.0, 0.0],
    })

    result = retrieve_features(entity_df, ["station_demand_15m"])

    assert "label" in result.columns
    assert list(result["label"]) == [1.0, 0.0]


def test_exact_timestamp_match(feature_parquet):
    exact_ts = datetime(2026, 1, 1, 2, 0)
    entity_df = pd.DataFrame({
        "station_id": ["station_001"],
        "ts_15m": [exact_ts],
    })

    exact_feature_row = feature_parquet[
        (feature_parquet["station_id"] == "station_001")
        & (feature_parquet["ts_15m"] == exact_ts)
    ]
    assert len(exact_feature_row) == 1, "test setup: exact row must exist in parquet"

    result = retrieve_features(entity_df, ["station_demand_15m"])

    for col in FEATURE_COLS:
        expected = exact_feature_row.iloc[0][col]
        actual = result.iloc[0][col]
        if pd.isna(expected):
            assert pd.isna(actual)
        else:
            assert abs(actual - expected) < 1e-9, (
                f"{col}: expected exact-match value {expected}, got {actual}"
            )


def test_row_order_preserved():
    entity_df = pd.DataFrame({
        "station_id": ["station_003", "station_001", "station_005", "station_001"],
        "ts_15m": [
            datetime(2026, 1, 5, 8, 0),
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 3, 12, 0),
            datetime(2026, 1, 7, 23, 45),
        ],
    })

    result = retrieve_features(entity_df, ["station_demand_15m"])

    assert list(result["station_id"]) == ["station_003", "station_001", "station_005", "station_001"]
    assert list(result["ts_15m"]) == list(entity_df["ts_15m"])
