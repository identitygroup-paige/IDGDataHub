import argparse
import json
from pathlib import Path

from sqlalchemy.orm import relationship

from connectors.snowflake import get_snowflake_connection
from semantic.metadata import SemanticMetadata
from semantic.naming import make_bridge_view_name, make_summary_view_name
from semantic.renderer import render_template


BRIDGE_TEMPLATE = "semantic/templates/relationship_bridge.sql"
SUMMARY_TEMPLATE = "semantic/templates/relationship_summary.sql"


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
        key = (
            row["FROM_TABLE"],
            row["FROM_COLUMN"],
            row["TO_TABLE"],
            row["TO_COLUMN"],
        )
        lookup[key] = row["CARDINALITY_TYPE"]

    return lookup


def build_context(
    relationship: dict,
    source_system: str,
    target_database: str,
    raw_schema: str,
    semantic_schema: str,
    cardinality_type: str,
    view_name: str,
) -> dict:
    return {
        "target_database": target_database,
        "semantic_schema": semantic_schema,
        "raw_schema": raw_schema,
        "view_name": view_name,
        "from_table": relationship["from_table"],
        "from_column": relationship["from_column"],
        "to_table": relationship["to_table"],
        "to_column": relationship["to_column"],
        "cardinality_type": cardinality_type,
        "source_system": source_system,
    }


def generate_sql(
    model: dict,
    target_database: str,
    raw_schema: str,
    semantic_schema: str,
) -> str:
    sf_conn = get_snowflake_connection(
        database=target_database,
        schema=semantic_schema,
    )

    try:
        metadata = SemanticMetadata(sf_conn)
        source_system = model["source_system"]

        cardinality_df = metadata.get_cardinality(source_system)
        cardinality_lookup = build_cardinality_lookup(cardinality_df)

        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {target_database}.{semantic_schema};"
        ]

        generated_count = 0
        skipped_count = 0

        for relationship in model["relationships"]:
            if relationship["confidence_label"] not in {"HIGH", "MEDIUM"}:
                skipped_count += 1
                continue

            key = relationship_key(relationship)
            cardinality_type = cardinality_lookup.get(key)

            if cardinality_type != "N:M":
                skipped_count += 1
                print(
                    "Skipping non-N:M relationship: "
                    f"{relationship['from_table']}.{relationship['from_column']} → "
                    f"{relationship['to_table']}.{relationship['to_column']} "
                    f"({cardinality_type or 'UNKNOWN'})"
                )
                continue

            bridge_view_name = make_bridge_view_name(
                from_table=relationship["from_table"],
                to_table=relationship["to_table"],
                source_system=source_system,
                join_column=relationship["from_column"],
            )

            summary_view_name = make_summary_view_name(
                from_table=relationship["from_table"],
                to_table=relationship["to_table"],
                source_system=source_system,
                join_column=relationship["from_column"],
            )

            bridge_context = build_context(
                relationship=relationship,
                source_system=source_system,
                target_database=target_database,
                raw_schema=raw_schema,
                semantic_schema=semantic_schema,
                cardinality_type=cardinality_type,
                view_name=bridge_view_name,
            )

            summary_context = build_context(
                relationship=relationship,
                source_system=source_system,
                target_database=target_database,
                raw_schema=raw_schema,
                semantic_schema=semantic_schema,
                cardinality_type=cardinality_type,
                view_name=summary_view_name,
            )

            statements.append(render_template(BRIDGE_TEMPLATE, bridge_context))
            statements.append(render_template(SUMMARY_TEMPLATE, summary_context))

            generated_count += 2

        print(f"Generated N:M bridge/summary views: {generated_count}")
        print(f"Skipped relationships: {skipped_count}")

        return "\n\n".join(statements) + "\n"

    finally:
        sf_conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--target-database", default="DATA_VALIDATION")
    parser.add_argument("--raw-schema", default="RAW_REDSHIFT")
    parser.add_argument("--semantic-schema", default="SEMANTIC")
    parser.add_argument("--output", required=False)
    args = parser.parse_args()

    model_path = Path(args.model)
    model = json.loads(model_path.read_text())

    output_path = Path(
        args.output
        or f"semantic/{model['source_system'].lower()}_nm_relationship_views.sql"
    )

    sql = generate_sql(
        model=model,
        target_database=args.target_database,
        raw_schema=args.raw_schema,
        semantic_schema=args.semantic_schema,
    )

    output_path.write_text(sql)
    print(f"Wrote SQL to {output_path}")


if __name__ == "__main__":
    main()