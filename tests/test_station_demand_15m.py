import os
import tempfile
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pytest

from features.transforms.station_demand_15m import EXPECTED_COLUMNS, build


@pytest.fixture
def synthetic_source():
    rows = []
    start = datetime(2026, 1, 1)
    for station_id in ["st_A", "st_B"]:
        for i in range(200):
            rows.append({
                "station_id": station_id,
                "ts_15m": start + timedelta(minutes=15 * i),
                "rides_started": float(i % 13),
                "rides_ended": float(i % 7),
            })

    df = pd.DataFrame(rows).sort_values(["station_id", "ts_15m"]).reset_index(drop=True)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        df.to_parquet(f.name, index=False)
        yield f.name, df

    os.unlink(f.name)


def test_output_columns(synthetic_source):
    path, _ = synthetic_source
    conn = duckdb.connect()
    result = build(path, conn)
    conn.close()

    assert list(result.columns) == EXPECTED_COLUMNS


def test_row_count_preserved(synthetic_source):
    path, source_df = synthetic_source
    conn = duckdb.connect()
    result = build(path, conn)
    conn.close()

    assert len(result) == len(source_df)


def test_lag_correctness(synthetic_source):
    path, source_df = synthetic_source
    conn = duckdb.connect()
    result = build(path, conn)
    conn.close()

    st_a_source = source_df[source_df["station_id"] == "st_A"].reset_index(drop=True)
    st_a_result = result[result["station_id"] == "st_A"].reset_index(drop=True)

    for offset in range(1, 10):
        idx = offset + 5
        assert st_a_result.loc[idx, "rides_started_lag_1"] == st_a_source.loc[idx - 1, "rides_started"]


def test_rolling_mean_correctness(synthetic_source):
    path, source_df = synthetic_source
    conn = duckdb.connect()
    result = build(path, conn)
    conn.close()

    st_a_source = source_df[source_df["station_id"] == "st_A"].reset_index(drop=True)
    st_a_result = result[result["station_id"] == "st_A"].reset_index(drop=True)

    idx = 10
    expected_mean_4 = st_a_source.loc[idx - 4 : idx - 1, "rides_started"].mean()
    assert abs(st_a_result.loc[idx, "rides_started_rolling_mean_4"] - expected_mean_4) < 1e-9


def test_feature_dtypes(synthetic_source):
    path, _ = synthetic_source
    conn = duckdb.connect()
    result = build(path, conn)
    conn.close()

    feature_cols = EXPECTED_COLUMNS[2:]
    for col in feature_cols:
        assert result[col].dtype == "float64", f"{col} has dtype {result[col].dtype}"
