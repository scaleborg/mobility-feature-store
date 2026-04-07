import argparse
import importlib
import os
import shutil
import uuid

import duckdb

from features.definitions.loader import load_feature_views
from registry.metadata_store import MetadataStore

CONFIG_DIR = "configs/feature_views"
DB_PATH = "data/metadata/feature_registry.db"

SOURCE_PATHS = {
    "curated.station_activity_15m": "data/curated/station_activity_15m.parquet",
}


def resolve_entrypoint(entrypoint: str):
    module_path, func_name = entrypoint.rsplit(":", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def materialize_one(spec, store):
    run_id = str(uuid.uuid4())
    offline_path = os.path.join(spec.offline_store["base_path"], "part-0.parquet")
    snapshot_path = spec.online_store["snapshot_path"]

    try:
        source_table = spec.source_tables[0]
        source_path = SOURCE_PATHS[source_table]
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"source not found: {source_path}")

        build_fn = resolve_entrypoint(spec.transformation_entrypoint)
        conn = duckdb.connect()
        df = build_fn(source_path, conn)
        conn.close()

        expected_cols = spec.entity_keys + [f.name for f in spec.feature_schema]
        actual_cols = list(df.columns)
        if actual_cols != expected_cols:
            raise ValueError(
                f"column mismatch for {spec.name}\n"
                f"  expected: {expected_cols}\n"
                f"  actual:   {actual_cols}"
            )

        os.makedirs(os.path.dirname(offline_path), exist_ok=True)
        df.to_parquet(offline_path, index=False)

        os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
        shutil.copy2(offline_path, snapshot_path)

        store.record_materialization(spec.name, spec.version, run_id, "success", offline_path)
        print(f"  materialized {spec.name}:{spec.version} -> {offline_path} ({len(df)} rows)")

    except Exception as exc:
        store.record_materialization(spec.name, spec.version, run_id, "failed", offline_path or "")
        raise RuntimeError(f"materialization failed for {spec.name}:{spec.version}") from exc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-view", default=None)
    parser.add_argument("--version", default=None)
    args = parser.parse_args()

    os.makedirs("data/metadata", exist_ok=True)
    store = MetadataStore(DB_PATH)

    feature_views = load_feature_views(CONFIG_DIR)

    for key, spec in feature_views.items():
        if args.feature_view and spec.name != args.feature_view:
            continue
        if args.version and spec.version != args.version:
            continue

        print(f"materializing {key} ...")
        materialize_one(spec, store)


if __name__ == "__main__":
    main()
