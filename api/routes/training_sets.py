import pandas as pd
from fastapi import APIRouter

from api.schemas import TrainingSetRequest, df_to_records
from scripts.retrieve import retrieve_features

router = APIRouter()


@router.post("/training-set/retrieve")
def retrieve(req: TrainingSetRequest):
    entity_df = pd.DataFrame(req.entities)
    if "ts_15m" in entity_df.columns:
        entity_df["ts_15m"] = pd.to_datetime(entity_df["ts_15m"])
    result_df = retrieve_features(entity_df, req.feature_views)
    return {"results": df_to_records(result_df)}
