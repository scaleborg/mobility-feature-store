from fastapi import APIRouter, HTTPException

from features.definitions.loader import load_feature_views

router = APIRouter(prefix="/feature-views")

CONFIG_DIR = "configs/feature_views"


@router.get("")
def list_feature_views():
    specs = load_feature_views(CONFIG_DIR)
    summaries = []
    for spec in specs.values():
        summaries.append({
            "name": spec.name,
            "version": spec.version,
            "description": spec.description,
            "owner": spec.owner,
            "entity_keys": spec.entity_keys,
            "tags": spec.tags,
            "feature_count": len(spec.feature_schema),
        })
    return {"feature_views": summaries}


@router.get("/{name}")
def get_feature_view(name: str):
    specs = load_feature_views(CONFIG_DIR)
    for spec in specs.values():
        if spec.name == name:
            return {
                "name": spec.name,
                "version": spec.version,
                "description": spec.description,
                "owner": spec.owner,
                "entity_keys": spec.entity_keys,
                "event_timestamp_col": spec.event_timestamp_col,
                "tags": spec.tags,
                "ttl": spec.ttl,
                "freshness_sla": spec.freshness_sla,
                "feature_schema": [
                    {"name": f.name, "dtype": f.dtype, "description": f.description}
                    for f in spec.feature_schema
                ],
            }
    raise HTTPException(status_code=404, detail=f"feature view not found: {name}")
