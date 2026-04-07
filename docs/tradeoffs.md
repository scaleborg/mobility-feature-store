# Tradeoffs

## Why DuckDB over Spark

- Single-machine, in-process — no cluster, no JVM, no infrastructure to manage.
- Native `ASOF JOIN` — Spark does not have this; equivalent logic requires a window function + filter workaround that is harder to verify for correctness.
- First-class Parquet read/write with zero-copy integration to pandas and Arrow.
- Fast enough for datasets that fit in memory (millions of rows, sub-second queries).
- **Trade**: won't scale to multi-TB without rearchitecting to a distributed engine.

## Why Parquet files, no warehouse

- No database server to provision, configure, or maintain.
- Portable — works on a laptop, in CI, or backed by cloud object storage.
- Columnar and compressed — efficient for analytical feature reads.
- **Trade**: no concurrent writes, no ACID transactions on the store. A single writer is assumed.

## Why no Feast

Feast enforces an online KV + offline warehouse split that is unnecessary here. This repo uses a single Parquet-backed store with explicit retrieval logic.

Feast's abstractions — feature services, provider backends, materialization jobs — add indirection without benefit at this scale. The core contracts (materialize, retrieve point-in-time, retrieve latest) are implemented directly in ~200 lines of Python + SQL.

- **Trade**: no built-in online KV store (Redis, DynamoDB), no managed infrastructure integration, no provider ecosystem.

## Current limitations

- **No streaming ingestion** — batch materialization only. Features are refreshed by re-running the materialization pipeline.
- **No incremental backfill** — each materialization run is a full refresh. No delta/append mode.
- **No online KV store** — the latest snapshot is a Parquet file, not a low-latency key-value cache. Acceptable for moderate QPS; not suitable for p99 < 10ms requirements.
- **No multi-version management** — one active version per feature view. No A/B feature serving or gradual rollout.
- **No feature monitoring** — no drift detection, no data quality checks beyond schema validation.
- **No access control** — single-user, single-project scope. No multi-tenancy or permission model.
