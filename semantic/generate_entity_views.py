import argparse
import json
from pathlib import Path

from semantic.renderer import render_template


TEMPLATE_PATH = "semantic/templates/entity_view.sql"


def build_entity_view_sql(
    entity: dict,
    target_database: str,
    raw_schema: str,
    semantic_schema: str,
) -> str:
    return render_template(
        TEMPLATE_PATH,
        {
            "target_database": target_database,
            "semantic_schema": semantic_schema,
            "raw_schema": raw_schema,
            "view_name": entity["view_name"],
            "entity_name": entity["entity_name"],
            "source_table": entity["source_table"],
        },
    )


def generate_sql(model: dict, target_database: str, raw_schema: str, semantic_schema: str):
    statements = [f"CREATE SCHEMA IF NOT EXISTS {target_database}.{semantic_schema};"]

    for entity in model["entities"]:
        statements.append(
            build_entity_view_sql(
                entity=entity,
                target_database=target_database,
                raw_schema=raw_schema,
                semantic_schema=semantic_schema,
            )
        )

    return "\n\n".join(statements) + "\n"


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
        or f"semantic/{model['source_system'].lower()}_entity_views.sql"
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