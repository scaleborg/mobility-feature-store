# Architecture

## Data flow

```
curated source (parquet)
        │
        ▼
  materialization
  (DuckDB window functions)
        │
        ├──► offline store (parquet files)
        │         │
        │         └──► historical retrieval (ASOF JOIN)
        │
        └──► online snapshot (latest parquet)
                  │
                  └──► latest retrieval (LEFT JOIN)
```

1. **Curated source** — a Parquet file representing cleaned, 15-minute-bucketed activity data per station.
2. **Materialization** — a DuckDB SQL transform computes lag and rolling features via window functions. Output is written to Parquet in the offline store and copied to an online snapshot.
3. **Retrieval** — two paths serve different use cases, both reading from Parquet via DuckDB.

## Feature view concept

A feature view is a declarative YAML spec defining:

- **Entity keys** — the grain of the feature table (e.g., `station_id`, `ts_15m`)
- **Event timestamp column** — the column used for point-in-time alignment
- **Feature schema** — names, dtypes, descriptions of computed features
- **Source tables** — upstream data dependency
- **Transformation entrypoint** — a `module.path:function` string resolved at runtime via `importlib`
- **Storage paths** — offline base path and online snapshot path
- **Governance** — owner, TTL, freshness SLA, tags

Specs are validated against Pydantic models (`FeatureViewSpec`, `FeatureField`) at load time. Invalid configs fail fast.

## Registry

SQLite-backed with two tables:

- **feature_views** — `(name, version, description, owner, created_at)`. Populated by `scripts/register_feature_views.py`.
- **materializations** — `(name, version, run_id, status, storage_path, created_at)`. Written by the materialization runner on each run.

`FeatureCatalog` wraps `MetadataStore` for the registration workflow.

## Retrieval paths

### Historical (training)

Input: entity DataFrame with `(station_id, ts_15m)`.

Uses DuckDB `ASOF LEFT JOIN` against the full offline Parquet. For each entity row, returns the feature row with the largest `feature.ts_15m <= entity.ts_15m` within the same station. Guarantees point-in-time correctness.

### Latest (serving)

Input: entity DataFrame with `(station_id)` only.

Selects the most recent row per station from the online snapshot via `ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY ts_15m DESC)`, then performs a standard `LEFT JOIN`.

Both paths preserve input row order via an internal `_entity_row_id` column that is added before the join and dropped after.

## API layer

FastAPI app mounted at `api.main:app`. Four route modules:

| Module | Endpoints |
|--------|-----------|
| `health` | `GET /health` |
| `feature_views` | `GET /feature-views`, `GET /feature-views/{name}` |
| `latest` | `POST /features/latest` |
| `training_sets` | `POST /training-set/retrieve` |

The API converts JSON request bodies to DataFrames, calls the existing retrieval functions, and converts the result back to JSON. NaN values are serialized as `null`.
