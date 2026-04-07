# Retrieval Semantics

## Point-in-time correctness

A naive equi-join on `(station_id, ts_15m)` between an entity table and a feature table can match future feature rows to past entity timestamps. This is temporal leakage — the model sees data during training that would not have been available at prediction time.

This system guarantees: for every entity row, features come from the latest available observation at or before the entity timestamp. No row in the output uses data from the future.

## Core invariant

For any entity row `(k, t)`, the returned feature row is:

```
argmax(t') such that t' ≤ t within partition k
```

where `k` is the entity key (e.g., `station_id`) and `t` is the entity timestamp (`ts_15m`).

## ASOF JOIN logic

DuckDB provides native `ASOF JOIN` syntax:

```sql
SELECT e.*, f.feature_1, f.feature_2, ...
FROM entity_df e
ASOF LEFT JOIN features f
    ON e.station_id = f.station_id
    AND e.ts_15m >= f.ts_15m
```

Properties:

- The join is **partitioned by entity key** (`station_id`). Each entity row is matched only against feature rows with the same key.
- For each entity row, the join selects the feature row with the **largest** `f.ts_15m` satisfying `f.ts_15m <= e.ts_15m`.
- **Timestamps are assumed monotonic** per partition in the offline store. The materialization step enforces this via `ORDER BY station_id, ts_15m`.
- `LEFT JOIN` semantics: entity rows with no matching feature (entity timestamp before any feature data) receive NULLs.

## Why exact timestamp match is correct

If `entity.ts_15m == feature.ts_15m`, the ASOF JOIN returns that exact feature row. This is correct because the features themselves are already lagged:

- `rides_started_lag_1` at time T contains the ride count from time T-1, not T.
- `rides_started_rolling_mean_4` at time T is the average of T-4 through T-1.

The transform guarantees no current-bucket data leaks into the feature values. Returning the same-time feature row does not introduce leakage.

## Row order preservation

The retrieval function adds an internal `_entity_row_id` column (0-based integer index) to the entity DataFrame before registering it in DuckDB. This column flows through the join. After the query, the result is sorted by `_entity_row_id` and the column is dropped.

This guarantees the output row order matches the input row order, regardless of how the entity DataFrame is sorted.

## Training retrieval vs online retrieval

| | Training (historical) | Online (latest) |
|---|---|---|
| **Entity input** | `(station_id, ts_15m)` | `(station_id)` |
| **Join type** | ASOF LEFT JOIN | LEFT JOIN |
| **Data source** | Full offline Parquet history | Latest-per-entity from online snapshot |
| **Timestamp handling** | Point-in-time: `feature.ts <= entity.ts` | No timestamp in query; uses most recent row per entity |
| **Use case** | Building ML training/evaluation datasets | Real-time scoring, dashboards |
| **Leakage risk** | Eliminated by ASOF semantics | Not applicable (no temporal dimension in query) |

Both paths use DuckDB for the join and return a DataFrame with the same row count as the input entity DataFrame.
