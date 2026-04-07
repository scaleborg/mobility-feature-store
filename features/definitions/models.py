from typing import List, Optional
from pydantic import BaseModel, Field


class FeatureField(BaseModel):
    name: str
    dtype: str
    description: Optional[str] = None


class FeatureViewSpec(BaseModel):
    name: str
    version: str
    description: Optional[str] = None

    entity_keys: List[str]
    event_timestamp_col: str

    source_tables: List[str]

    owner: str
    ttl: Optional[str] = None
    freshness_sla: Optional[str] = None

    tags: List[str] = Field(default_factory=list)

    feature_schema: List[FeatureField]

    transformation_entrypoint: str
    backfill_start: Optional[str] = None

    offline_store: dict
    online_store: dict


class EntityAttribute(BaseModel):
    name: str
    dtype: str
    required: bool = True


class EntitySpec(BaseModel):
    name: str
    description: Optional[str] = None

    entity_keys: List[str]
    primary_key: str

    attributes: List[EntityAttribute]

    ownership: dict
    tags: List[str] = Field(default_factory=list)
