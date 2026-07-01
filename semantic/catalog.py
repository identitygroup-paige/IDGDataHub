import json
from pathlib import Path


class SemanticCatalog:
    def __init__(self, catalog_path: str | Path):
        self.catalog_path = Path(catalog_path)
        self.data = json.loads(self.catalog_path.read_text())

    @property
    def source_system(self) -> str:
        return self.data["source_system"]

    @property
    def generated_at(self) -> str:
        return self.data["generated_at"]

    def entity_views(self) -> list[dict]:
        return self.data.get("entity_views", [])

    def safe_relationship_views(self) -> list[dict]:
        return self.data.get("safe_relationship_views", [])

    def bridge_views(self) -> list[dict]:
        return self.data.get("bridge_views", [])

    def summary_views(self) -> list[dict]:
        return self.data.get("summary_views", [])

    def skipped_relationships(self) -> list[dict]:
        return self.data.get("skipped_relationships", [])

    def relationships(self) -> list[dict]:
        return (
            self.safe_relationship_views()
            + self.bridge_views()
            + self.summary_views()
            + self.skipped_relationships()
        )

    def counts(self) -> dict:
        return {
            "entity_views": len(self.entity_views()),
            "safe_relationship_views": len(self.safe_relationship_views()),
            "bridge_views": len(self.bridge_views()),
            "summary_views": len(self.summary_views()),
            "skipped_relationships": len(self.skipped_relationships()),
        }