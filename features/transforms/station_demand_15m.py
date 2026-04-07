import duckdb
import pandas as pd


EXPECTED_COLUMNS = [
    "station_id",
    "ts_15m",
    "rides_started_lag_1",
    "rides_started_lag_4",
    "rides_started_lag_96",
    "rides_started_rolling_mean_4",
    "rides_started_rolling_mean_96",
    "rides_ended_lag_1",
]


def build(source_path: str, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    conn.execute(
        f"CREATE OR REPLACE VIEW source AS SELECT * FROM read_parquet('{source_path}')"
    )

    df = conn.execute("""
        SELECT
            station_id,
            ts_15m,
            LAG(rides_started, 1) OVER w AS rides_started_lag_1,
            LAG(rides_started, 4) OVER w AS rides_started_lag_4,
            LAG(rides_started, 96) OVER w AS rides_started_lag_96,
            AVG(rides_started) OVER (
                PARTITION BY station_id ORDER BY ts_15m
                ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) AS rides_started_rolling_mean_4,
            AVG(rides_started) OVER (
                PARTITION BY station_id ORDER BY ts_15m
                ROWS BETWEEN 96 PRECEDING AND 1 PRECEDING
            ) AS rides_started_rolling_mean_96,
            LAG(rides_ended, 1) OVER w AS rides_ended_lag_1
        FROM source
        WINDOW w AS (PARTITION BY station_id ORDER BY ts_15m)
        ORDER BY station_id, ts_15m
    """).df()

    return df[EXPECTED_COLUMNS]
