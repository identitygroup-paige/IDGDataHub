import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from connectors.snowflake import get_snowflake_connection
from semantic.metadata import SemanticMetadata
from semantic.naming import (
    make_bridge_view_name,
    make_relationship_view_name,
    make_summary_view_name,
)


def relationship_key(relationship: dict) -> tuple[str, str, str, str]:
    return (
        relationship["from_table"],
        relationship["from_column"],
        relationship["to_table"],
        relationship["to_column"],
    )


def build_cardinality_lookup(cardinality_df) -> dict:
    lookup = {}

    if cardinality_df.empty:
        return lookup

    for _, row in cardinality_df.iterrows():
        lookup[
            (
                row["FROM_TABLE"],
                row["FROM_COLUMN"],
                row["TO_TABLE"],
                row["TO_COLUMN"],
            )
        ] = row["CARDINALITY_TYPE"]

    return lookup


def build_catalog(model: dict, metadata: SemanticMetadata) -> dict:
    source_system = model["source_system"]
    cardinality_df = metadata.get_cardinality(source_system)
    cardinality_lookup = build_cardinality_lookup(cardinality_df)

    catalog = {
        "source_system": source_system,
        "generated_at": datetime.now(UTC).isoformat(),
        "entity_views": [],
        "safe_relationship_views": [],
        "bridge_views": [],
        "summary_views": [],
        "skipped_relationships": [],
    }

    for entity in model["entities"]:
        catalog["entity_views"].append(
            {
                "entity_name": entity["entity_name"],
                "source_table": entity["source_table"],
                "view_name": entity["view_name"],
                "primary_keys": entity.get("primary_keys", []),
                "business_keys": entity.get("business_keys", []),
                "unique_attributes": entity.get("unique_attributes", []),
            }
        )

    for relationship in model["relationships"]:
        key = relationship_key(relationship)
        cardinality_type = cardinality_lookup.get(key, "UNKNOWN")

        item = {
            "from_table": relationship["from_table"],
            "from_column": relationship["from_column"],
            "to_table": relationship["to_table"],
            "to_column": relationship["to_column"],
            "confidence_label": relationship["confidence_label"],
            "match_rate": relationship["match_rate"],
            "cardinality_type": cardinality_type,
        }

        if relationship["confidence_label"] not in {"HIGH", "MEDIUM"}:
            item["skip_reason"] = "confidence below generation threshold"
            catalog["skipped_relationships"].append(item)
            continue

        if cardinality_type == "1:1":
            item["view_name"] = make_relationship_view_name(
                relationship["from_table"],
                relationship["to_table"],
                source_system,
            )
            catalog["safe_relationship_views"].append(item)

        elif cardinality_type in {"1:N", "N:1", "N:M"}:
            bridge_item = item.copy()
            bridge_item["view_name"] = make_bridge_view_name(
    relationship["from_table"],
    relationship["to_table"],
    source_system,
    relationship["from_column"],
)

            summary_item = item.copy()
            summary_item["view_name"] = make_summary_view_name(
    relationship["from_table"],
    relationship["to_table"],
    source_system,
    relationship["from_column"],
)

            catalog["bridge_views"].append(bridge_item)
            catalog["summary_views"].append(summary_item)

            if cardinality_type == "N:M":
                skipped_item = item.copy()
                skipped_item["skip_reason"] = "N:M relationship is unsafe for flat joined view"
                skipped_item["unsafe_flat_view_name"] = make_relationship_view_name(
                    relationship["from_table"],
                    relationship["to_table"],
                    source_system,
                )
                catalog["skipped_relationships"].append(skipped_item)

        else:
            item["skip_reason"] = "unknown cardinality"
            catalog["skipped_relationships"].append(item)

    return catalog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--target-database", default="DATA_VALIDATION")
    parser.add_argument("--semantic-schema", default="SEMANTIC")
    parser.add_argument("--output", required=False)
    args = parser.parse_args()

    model_path = Path(args.model)
    model = json.loads(model_path.read_text())

    output_path = Path(
        args.output
        or f"semantic/{model['source_system'].lower()}_semantic_catalog.json"
    )

    sf_conn = get_snowflake_connection(
        database=args.target_database,
        schema=args.semantic_schema,
    )

    try:
        metadata = SemanticMetadata(sf_conn)
        catalog = build_catalog(model, metadata)
        output_path.write_text(json.dumps(catalog, indent=2))
        print(f"Wrote semantic catalog to {output_path}")

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()