import pandas as pd
from fastapi import APIRouter

from api.schemas import LatestRequest, df_to_records
from scripts.retrieve_latest import retrieve_latest_features

router = APIRouter()


@router.post("/features/latest")
def latest(req: LatestRequest):
    entity_df = pd.DataFrame(req.entities)
    result_df = retrieve_latest_features(entity_df, req.feature_views)
    return {"results": df_to_records(result_df)}
