import argparse
import json
from pathlib import Path

from connectors.snowflake import get_snowflake_connection
from semantic.metadata import SemanticMetadata
from semantic.naming import make_relationship_view_name
from semantic.relationship_resolver import build_flat_relationship_select
from semantic.renderer import render_template


OBJECT_TEMPLATE = "semantic/templates/relationship_view_object.sql"
FLAT_TEMPLATE = "semantic/templates/relationship_view_flat.sql"

SAFE_FLAT_CARDINALITIES = {"1:1"}
SUMMARY_CARDINALITIES = {"1:N", "N:1"}
SKIP_CARDINALITIES = {"N:M"}


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


def build_relationship_context(
    relationship: dict,
    source_system: str,
    target_database: str,
    raw_schema: str,
    semantic_schema: str,
    metadata: SemanticMetadata,
    mode: str,
) -> dict:
    from_table = relationship["from_table"]
    from_column = relationship["from_column"]
    to_table = relationship["to_table"]
    to_column = relationship["to_column"]

    view_name = make_relationship_view_name(
        from_table=from_table,
        to_table=to_table,
        source_system=source_system,
    )

    context = {
        "target_database": target_database,
        "semantic_schema": semantic_schema,
        "raw_schema": raw_schema,
        "view_name": view_name,
        "from_table": from_table,
        "from_column": from_column,
        "to_table": to_table,
        "to_column": to_column,
    }

    if mode == "flat":
        parent_columns = metadata.get_columns(source_system, from_table)
        child_columns = metadata.get_columns(source_system, to_table)

        select_parts = build_flat_relationship_select(
            parent_columns_df=parent_columns,
            child_columns_df=child_columns,
            child_table=to_table,
        )

        context.update(select_parts)

    return context


def build_relationship_view_sql(
    relationship: dict,
    source_system: str,
    target_database: str,
    raw_schema: str,
    semantic_schema: str,
    metadata: SemanticMetadata,
    mode: str,
) -> str:
    context = build_relationship_context(
        relationship=relationship,
        source_system=source_system,
        target_database=target_database,
        raw_schema=raw_schema,
        semantic_schema=semantic_schema,
        metadata=metadata,
        mode=mode,
    )

    template = FLAT_TEMPLATE if mode == "flat" else OBJECT_TEMPLATE
    return render_template(template, context)


def should_generate_relationship(
    relationship: dict,
    cardinality_type: str | None,
    mode: str,
) -> tuple[bool, str]:
    if relationship["confidence_label"] not in {"HIGH", "MEDIUM"}:
        return False, "confidence below generation threshold"

    if cardinality_type in SKIP_CARDINALITIES:
        return False, "N:M relationship requires bridge/manual modeling"

    if mode == "flat" and cardinality_type not in SAFE_FLAT_CARDINALITIES:
        return False, f"{cardinality_type or 'UNKNOWN'} is not safe for flat view"

    return True, "safe to generate"


def generate_sql(
    model: dict,
    target_database: str,
    raw_schema: str,
    semantic_schema: str,
    mode: str,
) -> str:
    sf_conn = get_snowflake_connection(
        database=target_database,
        schema=semantic_schema,
    )

    try:
        metadata = SemanticMetadata(sf_conn)
        source_system = model["source_system"]

        cardinality = metadata.get_cardinality(source_system)
        cardinality_lookup = build_cardinality_lookup(cardinality)

        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {target_database}.{semantic_schema};"
        ]

        generated_count = 0
        skipped_count = 0

        for relationship in model["relationships"]:
            key = relationship_key(relationship)
            cardinality_type = cardinality_lookup.get(key)

            should_generate, reason = should_generate_relationship(
                relationship=relationship,
                cardinality_type=cardinality_type,
                mode=mode,
            )

            if not should_generate:
                skipped_count += 1
                print(
                    "Skipping relationship view: "
                    f"{relationship['from_table']}.{relationship['from_column']} → "
                    f"{relationship['to_table']}.{relationship['to_column']} "
                    f"({cardinality_type or 'UNKNOWN'}): {reason}"
                )
                continue

            statements.append(
                build_relationship_view_sql(
                    relationship=relationship,
                    source_system=source_system,
                    target_database=target_database,
                    raw_schema=raw_schema,
                    semantic_schema=semantic_schema,
                    metadata=metadata,
                    mode=mode,
                )
            )
            generated_count += 1

        print(f"Generated relationship views: {generated_count}")
        print(f"Skipped relationship views: {skipped_count}")

        return "\n\n".join(statements) + "\n"

    finally:
        sf_conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--target-database", default="DATA_VALIDATION")
    parser.add_argument("--raw-schema", default="RAW_REDSHIFT")
    parser.add_argument("--semantic-schema", default="SEMANTIC")
    parser.add_argument("--mode", choices=["object", "flat"], default="flat")
    parser.add_argument("--output", required=False)
    args = parser.parse_args()

    model_path = Path(args.model)
    model = json.loads(model_path.read_text())

    output_path = Path(
        args.output
        or f"semantic/{model['source_system'].lower()}_relationship_views.sql"
    )

    sql = generate_sql(
        model=model,
        target_database=args.target_database,
        raw_schema=args.raw_schema,
        semantic_schema=args.semantic_schema,
        mode=args.mode,
    )

    output_path.write_text(sql)
    print(f"Wrote SQL to {output_path}")


if __name__ == "__main__":
    main()