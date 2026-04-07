from typing import Dict

from features.definitions.loader import load_feature_views
from registry.metadata_store import MetadataStore


class FeatureCatalog:
    def __init__(self, metadata_store: MetadataStore):
        self.store = metadata_store

    def register_from_configs(self, config_dir: str):
        feature_views = load_feature_views(config_dir)

        for spec in feature_views.values():
            self.store.register_feature_view(
                name=spec.name,
                version=spec.version,
                description=spec.description or "",
                owner=spec.owner,
            )

    def list_feature_views(self):
        return self.store.list_feature_views()
