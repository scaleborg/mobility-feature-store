import os

import duckdb
import pandas as pd
import pytest

SNAPSHOT_PATH = "data/online_snapshots/station_demand_15m_latest.parquet"

if not os.path.exists(SNAPSHOT_PATH):
    pytest.skip(
        f"Online snapshot not found at {SNAPSHOT_PATH}. "
        "Run: PYTHONPATH=. python3 scripts/materialize.py --feature-view station_demand_15m",
        allow_module_level=True,
    )

from scripts.retrieve_latest import retrieve_latest_features

FEATURE_COLS = [
    "rides_started_lag_1",
    "rides_started_lag_4",
    "rides_started_lag_96",
    "rides_started_rolling_mean_4",
    "rides_started_rolling_mean_96",
    "rides_ended_lag_1",
]


@pytest.fixture
def latest_per_station():
    conn = duckdb.connect()
    df = conn.execute(f"""
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY ts_15m DESC) AS _rn
            FROM read_parquet('{SNAPSHOT_PATH}')
        ) WHERE _rn = 1
    """).df().drop(columns=["_rn"])
    conn.close()
    return df.set_index("station_id")


def test_row_count_preserved():
    entity_df = pd.DataFrame({"station_id": ["station_001", "station_002", "station_003"]})
    result = retrieve_latest_features(entity_df, ["station_demand_15m"])
    assert len(result) == 3


def test_join_correctness(latest_per_station):
    entity_df = pd.DataFrame({"station_id": ["station_001"]})
    result = retrieve_latest_features(entity_df, ["station_demand_15m"])

    expected = latest_per_station.loc["station_001"]
    for col in FEATURE_COLS:
        exp_val = expected[col]
        act_val = result.iloc[0][col]
        if pd.isna(exp_val):
            assert pd.isna(act_val), f"{col}: expected NaN, got {act_val}"
        else:
            assert abs(act_val - exp_val) < 1e-9, f"{col}: expected {exp_val}, got {act_val}"


def test_missing_entity_returns_null():
    entity_df = pd.DataFrame({"station_id": ["station_999"]})
    result = retrieve_latest_features(entity_df, ["station_demand_15m"])

    assert len(result) == 1
    for col in FEATURE_COLS:
        assert pd.isna(result.iloc[0][col]), f"{col} should be NULL for unknown station"


def test_extra_entity_columns_preserved():
    entity_df = pd.DataFrame({
        "station_id": ["station_001", "station_002"],
        "priority": ["high", "low"],
    })
    result = retrieve_latest_features(entity_df, ["station_demand_15m"])

    assert "priority" in result.columns
    assert list(result["priority"]) == ["high", "low"]


def test_multiple_rows_same_entity(latest_per_station):
    entity_df = pd.DataFrame({
        "station_id": ["station_003", "station_003", "station_003"],
    })
    result = retrieve_latest_features(entity_df, ["station_demand_15m"])

    assert len(result) == 3

    expected = latest_per_station.loc["station_003"]
    for i in range(3):
        for col in FEATURE_COLS:
            exp_val = expected[col]
            act_val = result.iloc[i][col]
            if pd.isna(exp_val):
                assert pd.isna(act_val)
            else:
                assert abs(act_val - exp_val) < 1e-9
