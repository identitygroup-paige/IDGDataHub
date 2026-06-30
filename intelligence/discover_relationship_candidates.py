import argparse
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from connectors.snowflake import get_snowflake_connection

load_dotenv()


def get_key_candidates(sf_conn, source_system):
    query = """
        SELECT *
        FROM DATA_VALIDATION.INTELLIGENCE.KEY_CANDIDATES
        WHERE SOURCE_SYSTEM = %s
    """
    df = pd.read_sql(query, sf_conn, params=[source_system])
    df.columns = [col.upper() for col in df.columns]
    return df


def compatible_name(parent_column, child_column):
    parent = str(parent_column).upper()
    child = str(child_column).upper()

    if parent == child:
        return True

    if parent in child or child in parent:
        return True

    parent_base = (
        parent.replace("_ID", "")
        .replace("ID", "")
        .replace("_NUMBER", "")
        .replace("NUMBER", "")
        .replace("_CODE", "")
        .replace("CODE", "")
        .replace("_KEY", "")
        .replace("KEY", "")
    )

    child_base = (
        child.replace("_ID", "")
        .replace("ID", "")
        .replace("_NUMBER", "")
        .replace("NUMBER", "")
        .replace("_CODE", "")
        .replace("CODE", "")
        .replace("_KEY", "")
        .replace("KEY", "")
    )

    return parent_base and child_base and parent_base == child_base


def build_candidate_pairs(candidates):
    parents = candidates[
        candidates["CANDIDATE_TYPE"].isin(["PRIMARY_KEY", "BUSINESS_KEY", "UNIQUE_ATTRIBUTE"])
    ].copy()

    children = candidates[
        candidates["CANDIDATE_TYPE"].isin(["BUSINESS_KEY", "FOREIGN_KEY"])
    ].copy()

    pairs = []

    for _, parent in parents.iterrows():
        for _, child in children.iterrows():
            if parent["TARGET_TABLE"] == child["TARGET_TABLE"]:
                continue

            if not compatible_name(parent["TARGET_COLUMN"], child["TARGET_COLUMN"]):
                continue

            pairs.append((parent, child))

    return pairs


def test_relationship(sf_conn, relationship_run_id, parent, child):
    source_system = parent["SOURCE_SYSTEM"]
    profile_run_id = parent["PROFILE_RUN_ID"]

    parent_table = parent["TARGET_TABLE"]
    parent_column = parent["TARGET_COLUMN"]
    child_table = child["TARGET_TABLE"]
    child_column = child["TARGET_COLUMN"]

    database = parent["TARGET_DATABASE"]
    schema = parent["TARGET_SCHEMA"]

    query = f"""
        WITH child_values AS (
            SELECT DISTINCT "{child_column}" AS CHILD_VALUE
            FROM {database}.{schema}."{child_table}"
            WHERE "{child_column}" IS NOT NULL
        ),
        parent_values AS (
            SELECT DISTINCT "{parent_column}" AS PARENT_VALUE
            FROM {database}.{schema}."{parent_table}"
            WHERE "{parent_column}" IS NOT NULL
        )
        SELECT
            COUNT(c.CHILD_VALUE) AS CHILD_NON_NULL_COUNT,
            COUNT(p.PARENT_VALUE) AS MATCH_COUNT
        FROM child_values c
        LEFT JOIN parent_values p
          ON TO_VARCHAR(c.CHILD_VALUE) = TO_VARCHAR(p.PARENT_VALUE)
    """

    result = pd.read_sql(query, sf_conn)
    result.columns = [col.upper() for col in result.columns]

    child_non_null_count = int(result["CHILD_NON_NULL_COUNT"].iloc[0] or 0)
    match_count = int(result["MATCH_COUNT"].iloc[0] or 0)

    match_rate = (
        match_count / child_non_null_count
        if child_non_null_count
        else 0
    )

    if match_rate >= 0.98:
        confidence_label = "HIGH"
        confidence_score = 0.95
    elif match_rate >= 0.90:
        confidence_label = "MEDIUM"
        confidence_score = 0.80
    elif match_rate >= 0.70:
        confidence_label = "LOW"
        confidence_score = 0.60
    else:
        confidence_label = "WEAK"
        confidence_score = 0.30

    return {
        "RELATIONSHIP_RUN_ID": relationship_run_id,
        "PROFILE_RUN_ID": profile_run_id,
        "SOURCE_SYSTEM": source_system,
        "PARENT_TABLE": parent_table,
        "PARENT_COLUMN": parent_column,
        "CHILD_TABLE": child_table,
        "CHILD_COLUMN": child_column,
        "MATCH_COUNT": match_count,
        "CHILD_NON_NULL_COUNT": child_non_null_count,
        "MATCH_RATE": match_rate,
        "CONFIDENCE_SCORE": confidence_score,
        "CONFIDENCE_LABEL": confidence_label,
        "DISCOVERY_METHOD": "candidate_key_value_overlap",
        "CREATED_AT": datetime.now(UTC),
    }


def write_relationships(sf_conn, rows):
    if not rows:
        print("No relationship candidates discovered.")
        return

    df = pd.DataFrame(rows)

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="RELATIONSHIP_CANDIDATES",
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
        use_logical_type=True,
    )

    print(f"Inserted {len(df):,} relationship candidate row(s).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-system", required=True)
    parser.add_argument("--min-confidence-label", default="WEAK")
    args = parser.parse_args()

    relationship_run_id = str(uuid4())

    sf_conn = get_snowflake_connection(
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
    )

    try:
        candidates = get_key_candidates(sf_conn, args.source_system)
        print(f"Key candidates loaded: {len(candidates):,}")

        pairs = build_candidate_pairs(candidates)
        print(f"Candidate relationship pairs: {len(pairs):,}")
        print(f"Relationship run ID: {relationship_run_id}")

        rows = []

        for idx, (parent, child) in enumerate(pairs, start=1):
            print(
                f"[{idx}/{len(pairs)}] "
                f"{parent['TARGET_TABLE']}.{parent['TARGET_COLUMN']} → "
                f"{child['TARGET_TABLE']}.{child['TARGET_COLUMN']}"
            )

            try:
                relationship = test_relationship(
                    sf_conn=sf_conn,
                    relationship_run_id=relationship_run_id,
                    parent=parent,
                    child=child,
                )
                rows.append(relationship)
                print(
                    f"  match_rate={relationship['MATCH_RATE']:.4f} "
                    f"confidence={relationship['CONFIDENCE_LABEL']}"
                )
            except Exception as error:
                print(f"  ✗ Failed: {error}")

        write_relationships(sf_conn, rows)

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()