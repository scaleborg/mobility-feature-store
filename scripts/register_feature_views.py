from registry.metadata_store import MetadataStore
from registry.catalog import FeatureCatalog

CONFIG_DIR = "configs/feature_views"
DB_PATH = "data/metadata/feature_registry.db"


def main():
    store = MetadataStore(DB_PATH)
    catalog = FeatureCatalog(store)

    catalog.register_from_configs(CONFIG_DIR)

    print("Feature views registered:")
    for row in catalog.list_feature_views():
        print(row)


if __name__ == "__main__":
    main()
