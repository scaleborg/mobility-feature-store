import os
from datetime import datetime

import duckdb
import pandas as pd

from features.definitions.loader import load_feature_views

CONFIG_DIR = "configs/feature_views"


def retrieve_features(
    entity_df: pd.DataFrame,
    feature_views: list[str],
) -> pd.DataFrame:
    specs = load_feature_views(CONFIG_DIR)

    working_df = entity_df.copy()
    working_df["_entity_row_id"] = range(len(working_df))

    conn = duckdb.connect()
    conn.register("entity_df", working_df)

    for fv_name in feature_views:
        spec = None
        for s in specs.values():
            if s.name == fv_name:
                spec = s
                break
        if spec is None:
            raise ValueError(f"feature view not found: {fv_name}")

        parquet_path = os.path.join(spec.offline_store["base_path"], "part-0.parquet")
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(
                f"offline store not found for {fv_name}: {parquet_path}\n"
                f"Run materialization first: PYTHONPATH=. python3 scripts/materialize.py --feature-view {fv_name}"
            )

        feature_cols = [f.name for f in spec.feature_schema]
        event_ts_col = spec.event_timestamp_col
        entity_keys = [k for k in spec.entity_keys if k != event_ts_col]

        conn.execute(
            f"CREATE OR REPLACE VIEW features AS SELECT * FROM read_parquet('{parquet_path}')"
        )

        entity_cols = [c for c in working_df.columns if c != "_entity_row_id"]
        e_select = ", ".join(f"e.{c}" for c in ["_entity_row_id"] + entity_cols)
        f_select = ", ".join(f"f.{c}" for c in feature_cols)

        on_clauses = " AND ".join(f"e.{k} = f.{k}" for k in entity_keys)
        on_clauses += f" AND e.{event_ts_col} >= f.{event_ts_col}"

        sql = f"""
            SELECT {e_select}, {f_select}
            FROM entity_df e
            ASOF LEFT JOIN features f
                ON {on_clauses}
            ORDER BY e._entity_row_id
        """

        working_df = conn.execute(sql).df()
        conn.register("entity_df", working_df)

    conn.close()

    working_df = working_df.sort_values("_entity_row_id").reset_index(drop=True)
    working_df = working_df.drop(columns=["_entity_row_id"])

    return working_df


def main():
    entity_df = pd.DataFrame({
        "station_id": [
            "station_001", "station_001", "station_001", "station_001", "station_001",
            "station_003", "station_003", "station_003", "station_003", "station_003",
        ],
        "ts_15m": [
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 1, 0),
            datetime(2026, 1, 1, 6, 0),
            datetime(2026, 1, 3, 12, 0),
            datetime(2026, 1, 7, 23, 45),
            datetime(2026, 1, 1, 0, 0),
            datetime(2026, 1, 1, 2, 0),
            datetime(2026, 1, 2, 0, 0),
            datetime(2026, 1, 5, 8, 0),
            datetime(2026, 1, 7, 23, 45),
        ],
    })

    print("Entity DataFrame:")
    print(entity_df.to_string(index=False))
    print()

    result = retrieve_features(entity_df, ["station_demand_15m"])

    print("Retrieved Features:")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
