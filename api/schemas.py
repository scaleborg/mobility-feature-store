import math

import pandas as pd
from pydantic import BaseModel


class LatestRequest(BaseModel):
    feature_views: list[str]
    entities: list[dict]


class TrainingSetRequest(BaseModel):
    feature_views: list[str]
    entities: list[dict]


def df_to_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if isinstance(v, float) and math.isnan(v):
                clean[k] = None
            elif isinstance(v, pd.Timestamp):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        records.append(clean)
    return records
