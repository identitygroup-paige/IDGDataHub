import argparse
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from connectors.snowflake import get_snowflake_connection

load_dotenv()


def get_usable_relationships(sf_conn, source_system):
    query = """
        SELECT
            SOURCE_SYSTEM,
            FROM_TABLE,
            FROM_COLUMN,
            TO_TABLE,
            TO_COLUMN
        FROM DATA_VALIDATION.INTELLIGENCE.RELATIONSHIP_GRAPH
        WHERE SOURCE_SYSTEM = %s
          AND CONFIDENCE_LABEL IN ('HIGH', 'MEDIUM')
    """
    df = pd.read_sql(query, sf_conn, params=[source_system])
    df.columns = [col.upper() for col in df.columns]
    return df


def detect_cardinality(sf_conn, run_id, row):
    source_system = row["SOURCE_SYSTEM"]
    from_table = row["FROM_TABLE"]
    from_column = row["FROM_COLUMN"]
    to_table = row["TO_TABLE"]
    to_column = row["TO_COLUMN"]

    query = f"""
        WITH from_keys AS (
            SELECT
                NULLIF(TRIM(TO_VARCHAR("{from_column}")), '') AS key_value,
                COUNT(*) AS row_count
            FROM DATA_VALIDATION.RAW_REDSHIFT."{from_table}"
            WHERE NULLIF(TRIM(TO_VARCHAR("{from_column}")), '') IS NOT NULL
            GROUP BY 1
        ),
        to_keys AS (
            SELECT
                NULLIF(TRIM(TO_VARCHAR("{to_column}")), '') AS key_value,
                COUNT(*) AS row_count
            FROM DATA_VALIDATION.RAW_REDSHIFT."{to_table}"
            WHERE NULLIF(TRIM(TO_VARCHAR("{to_column}")), '') IS NOT NULL
            GROUP BY 1
        )
        SELECT
            (SELECT COUNT(*) FROM from_keys) AS FROM_DISTINCT_KEYS,
            (SELECT COUNT(*) FROM to_keys) AS TO_DISTINCT_KEYS,
            (SELECT COUNT(*) FROM from_keys WHERE row_count > 1) AS FROM_DUPLICATE_KEYS,
            (SELECT COUNT(*) FROM to_keys WHERE row_count > 1) AS TO_DUPLICATE_KEYS
    """

    result = pd.read_sql(query, sf_conn)
    result.columns = [col.upper() for col in result.columns]

    from_dup = int(result["FROM_DUPLICATE_KEYS"].iloc[0] or 0)
    to_dup = int(result["TO_DUPLICATE_KEYS"].iloc[0] or 0)

    if from_dup == 0 and to_dup == 0:
        cardinality_type = "1:1"
        reason = "both sides have unique join keys"
    elif from_dup == 0 and to_dup > 0:
        cardinality_type = "1:N"
        reason = "parent side is unique; child side has repeated join keys"
    elif from_dup > 0 and to_dup == 0:
        cardinality_type = "N:1"
        reason = "parent side has repeated join keys; child side is unique"
    else:
        cardinality_type = "N:M"
        reason = "both sides have repeated join keys"

    return {
        "CARDINALITY_RUN_ID": run_id,
        "SOURCE_SYSTEM": source_system,
        "FROM_TABLE": from_table,
        "FROM_COLUMN": from_column,
        "TO_TABLE": to_table,
        "TO_COLUMN": to_column,
        "FROM_DISTINCT_KEYS": int(result["FROM_DISTINCT_KEYS"].iloc[0] or 0),
        "TO_DISTINCT_KEYS": int(result["TO_DISTINCT_KEYS"].iloc[0] or 0),
        "FROM_DUPLICATE_KEYS": from_dup,
        "TO_DUPLICATE_KEYS": to_dup,
        "CARDINALITY_TYPE": cardinality_type,
        "REASON": reason,
        "CREATED_AT": datetime.now(UTC),
    }


def write_results(sf_conn, rows):
    if not rows:
        print("No cardinality rows to write.")
        return

    df = pd.DataFrame(rows)

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="RELATIONSHIP_CARDINALITY",
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
        use_logical_type=True,
    )

    print(f"Inserted {len(df):,} cardinality row(s).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-system", required=True)
    args = parser.parse_args()

    run_id = str(uuid4())

    sf_conn = get_snowflake_connection(
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
    )

    try:
        relationships = get_usable_relationships(sf_conn, args.source_system)
        print(f"Relationships loaded: {len(relationships):,}")
        print(f"Cardinality run ID: {run_id}")

        rows = []

        for _, row in relationships.iterrows():
            detected = detect_cardinality(sf_conn, run_id, row)
            rows.append(detected)
            print(
                f"{detected['FROM_TABLE']}.{detected['FROM_COLUMN']} → "
                f"{detected['TO_TABLE']}.{detected['TO_COLUMN']}: "
                f"{detected['CARDINALITY_TYPE']}"
            )

        write_results(sf_conn, rows)

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()