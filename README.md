# mobility-feature-store

Stores and serves point-in-time feature datasets for training and inference in the mobility ML platform.

This layer materializes features from the pipeline into reproducible offline datasets and lightweight online snapshots, enforcing strict consistency between training and serving.

---

## Role in the ML Platform

P3 sits between feature computation (P2) and model training / serving (P4 / P5).

It is responsible for:

- storing features with point-in-time guarantees
- enabling reproducible training dataset extraction
- exposing consistent feature views for offline and online use
- enforcing feature contract stability across the system

---

## Data flow

curated source (parquet)  
        │  
        ▼  
materialization (DuckDB SQL)  
        │  
        ├──► offline store (historical parquet datasets)  
        │         │  
        │         └──► point-in-time retrieval (ASOF JOIN)  
        │  
        └──► online snapshot (latest parquet)  
                  │  
                  └──► low-latency lookup (LEFT JOIN)  

---

## Storage layout

- offline store  
  Versioned Parquet datasets used for training and evaluation

- online snapshot  
  Latest feature state per entity for inference

All data is stored as Parquet files with explicit schema contracts.

---

## Point-in-time retrieval

Offline datasets are built using ASOF JOIN semantics:

- join on entity_id
- retrieve the latest feature row where feature_ts ≤ observation_ts
- prevent any leakage from future data

This guarantees that training data matches what would have been available at prediction time.

---

## Feature contract

Each dataset enforces:

- stable column names and types
- explicit entity_id and event_ts fields
- deterministic feature computation logic (from P2)

Training and serving both rely on the same contract.

---

## Usage

### Export point-in-time dataset

Produces a training dataset from historical data:

make export

Output:
- Parquet dataset in `exports/<dataset>/<date>/`
- includes metadata (build time, row count, feature schema)

---

### Load features for training (P4)

P4 reads directly from exported Parquet datasets:

- no recomputation
- fully reproducible inputs

---

### Load features for serving (P5)

P5 reads from:

- latest snapshot parquet
- DuckDB read-only connection

Used for real-time scoring.

---

## Design principles

- no feature recomputation outside P2
- offline and online paths share the same source data
- strict separation between feature computation and feature serving
- reproducibility over convenience

---

## What this layer proves in the system

- point-in-time correct feature retrieval
- reproducible training datasets
- consistent offline / online feature access
- contract-driven feature serving
