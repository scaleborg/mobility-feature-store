import os
import random
from datetime import datetime, timedelta

import pandas as pd


OUTPUT_PATH = "data/curated/station_activity_15m.parquet"

STATIONS = [f"station_{i:03d}" for i in range(1, 6)]
DAYS = 7
BUCKETS_PER_DAY = 96
START = datetime(2026, 1, 1)
SEED = 42


def main():
    random.seed(SEED)

    rows = []
    for station_id in STATIONS:
        for bucket_idx in range(DAYS * BUCKETS_PER_DAY):
            ts = START + timedelta(minutes=15 * bucket_idx)
            rows.append({
                "station_id": station_id,
                "ts_15m": ts,
                "rides_started": float(random.randint(0, 20)),
                "rides_ended": float(random.randint(0, 20)),
            })

    df = pd.DataFrame(rows).sort_values(["station_id", "ts_15m"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"wrote {len(df)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
