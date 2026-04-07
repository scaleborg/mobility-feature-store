import os

import duckdb
import pandas as pd

from features.definitions.loader import load_feature_views

CONFIG_DIR = "configs/feature_views"


def retrieve_latest_features(
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

        snapshot_path = spec.online_store["snapshot_path"]
        if not os.path.exists(snapshot_path):
            raise FileNotFoundError(
                f"online snapshot not found for {fv_name}: {snapshot_path}\n"
                f"Run materialization first: PYTHONPATH=. python3 scripts/materialize.py --feature-view {fv_name}"
            )

        feature_cols = [f.name for f in spec.feature_schema]
        event_ts_col = spec.event_timestamp_col
        entity_keys = [k for k in spec.entity_keys if k != event_ts_col]

        f_select = ", ".join(f"latest.{c}" for c in feature_cols)
        on_clause = " AND ".join(f"e.{k} = latest.{k}" for k in entity_keys)

        entity_cols = [c for c in working_df.columns if c != "_entity_row_id"]
        e_select = ", ".join(f"e.{c}" for c in ["_entity_row_id"] + entity_cols)

        sql = f"""
            WITH latest AS (
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY {', '.join(entity_keys)}
                            ORDER BY {event_ts_col} DESC
                        ) AS _rn
                    FROM read_parquet('{snapshot_path}')
                )
                WHERE _rn = 1
            )
            SELECT {e_select}, {f_select}
            FROM entity_df e
            LEFT JOIN latest
                ON {on_clause}
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
            "station_001",
            "station_002",
            "station_003",
            "station_004",
            "station_005",
            "station_999",
        ],
    })

    print("Entity DataFrame:")
    print(entity_df.to_string(index=False))
    print()

    result = retrieve_latest_features(entity_df, ["station_demand_15m"])

    print("Latest Features:")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
