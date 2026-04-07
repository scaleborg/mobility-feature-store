import os
import yaml
from typing import Dict

from features.definitions.models import FeatureViewSpec, EntitySpec


def _load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_feature_views(config_dir: str) -> Dict[str, FeatureViewSpec]:
    feature_views = {}

    for filename in os.listdir(config_dir):
        if not filename.endswith(".yaml"):
            continue

        path = os.path.join(config_dir, filename)
        raw = _load_yaml(path)

        spec = FeatureViewSpec(**raw)
        key = f"{spec.name}:{spec.version}"

        feature_views[key] = spec

    return feature_views


def load_entities(config_dir: str) -> Dict[str, EntitySpec]:
    entities = {}

    for filename in os.listdir(config_dir):
        if not filename.endswith(".yaml"):
            continue

        path = os.path.join(config_dir, filename)
        raw = _load_yaml(path)

        spec = EntitySpec(**raw)
        entities[spec.name] = spec

    return entities
