import argparse
import json
from pathlib import Path
from datetime import UTC, datetime

import pandas as pd
from dotenv import load_dotenv

from connectors.snowflake import get_snowflake_connection
from semantic.semantic_rules import infer_entity_name, make_view_name

load_dotenv()


def get_entity_map(sf_conn, source_system: str):
    query = """
        SELECT *
        FROM DATA_VALIDATION.INTELLIGENCE.ENTITY_MAP
        WHERE SOURCE_SYSTEM = %s
    """
    df = pd.read_sql(query, sf_conn, params=[source_system])
    df.columns = [col.upper() for col in df.columns]
    return df


def get_relationship_graph(sf_conn, source_system: str):
    query = """
        SELECT *
        FROM DATA_VALIDATION.INTELLIGENCE.RELATIONSHIP_GRAPH
        WHERE SOURCE_SYSTEM = %s
    """
    df = pd.read_sql(query, sf_conn, params=[source_system])
    df.columns = [col.upper() for col in df.columns]
    return df


def get_key_candidates(sf_conn, source_system: str):
    query = """
        SELECT *
        FROM DATA_VALIDATION.INTELLIGENCE.KEY_CANDIDATES
        WHERE SOURCE_SYSTEM = %s
    """
    df = pd.read_sql(query, sf_conn, params=[source_system])
    df.columns = [col.upper() for col in df.columns]
    return df


def build_semantic_model(source_system, entity_map, relationships, key_candidates):
    model = {
        "source_system": source_system,
        "generated_at": datetime.now(UTC).isoformat(),
        "entities": [],
        "relationships": [],
    }

    tables = sorted(entity_map["TARGET_TABLE"].dropna().unique())

    for table in tables:
        entity_name = infer_entity_name(table)

        table_keys = key_candidates[
            key_candidates["TARGET_TABLE"] == table
        ].sort_values("CONFIDENCE_SCORE", ascending=False)

        primary_keys = table_keys[
            table_keys["CANDIDATE_TYPE"] == "PRIMARY_KEY"
        ]["TARGET_COLUMN"].tolist()

        business_keys = table_keys[
            table_keys["CANDIDATE_TYPE"] == "BUSINESS_KEY"
        ]["TARGET_COLUMN"].tolist()

        unique_attributes = table_keys[
            table_keys["CANDIDATE_TYPE"] == "UNIQUE_ATTRIBUTE"
        ]["TARGET_COLUMN"].tolist()

        model["entities"].append(
            {
                "entity_name": entity_name,
                "source_table": table,
                "view_name": make_view_name(entity_name, source_system),
                "primary_keys": primary_keys,
                "business_keys": business_keys,
                "unique_attributes": unique_attributes,
            }
        )

    for _, row in relationships.iterrows():
        model["relationships"].append(
            {
                "from_table": row["FROM_TABLE"],
                "from_column": row["FROM_COLUMN"],
                "to_table": row["TO_TABLE"],
                "to_column": row["TO_COLUMN"],
                "relationship_type": row["RELATIONSHIP_TYPE"],
                "confidence_label": row["CONFIDENCE_LABEL"],
                "match_rate": float(row["MATCH_RATE"]),
            }
        )

    return model


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-system", required=True)
    parser.add_argument("--output", required=False)
    args = parser.parse_args()

    output_path = Path(
        args.output or f"semantic/{args.source_system.lower()}_semantic_model.json"
    )

    sf_conn = get_snowflake_connection(
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
    )

    try:
        entity_map = get_entity_map(sf_conn, args.source_system)
        relationships = get_relationship_graph(sf_conn, args.source_system)
        key_candidates = get_key_candidates(sf_conn, args.source_system)

        model = build_semantic_model(
            source_system=args.source_system,
            entity_map=entity_map,
            relationships=relationships,
            key_candidates=key_candidates,
        )

        output_path.write_text(json.dumps(model, indent=2))
        print(f"Wrote semantic model to {output_path}")

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()