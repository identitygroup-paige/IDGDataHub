import argparse
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from connectors.snowflake import get_snowflake_connection

load_dotenv()


def clean_sql_value(value):
    if value is None or pd.isna(value):
        return None
    return str(value)


def get_columns_to_profile(sf_conn, source_system=None):
    query = """
        SELECT
            SOURCE_SYSTEM,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            TARGET_COLUMN,
            TARGET_DATA_TYPE
        FROM DATA_VALIDATION.METADATA.V_COLUMN_CATALOG_LATEST
        WHERE 1 = 1
    """

    params = []

    if source_system:
        query += " AND SOURCE_SYSTEM = %s"
        params.append(source_system)

    query += """
        ORDER BY SOURCE_SYSTEM, TARGET_TABLE, TARGET_COLUMN
    """

    return pd.read_sql(query, sf_conn, params=params)


def profile_column(sf_conn, profile_run_id, row):
    source_system = row["SOURCE_SYSTEM"]
    database = row["TARGET_DATABASE"]
    schema = row["TARGET_SCHEMA"]
    table = row["TARGET_TABLE"]
    column = row["TARGET_COLUMN"]
    data_type = row["TARGET_DATA_TYPE"]

    query = f'''
        SELECT
            COUNT(*) AS ROW_COUNT,
            COUNT("{column}") AS NON_NULL_COUNT,
            COUNT(*) - COUNT("{column}") AS NULL_COUNT,
            COUNT(DISTINCT "{column}") AS DISTINCT_COUNT,
            MIN(TO_VARCHAR("{column}")) AS MIN_VALUE,
            MAX(TO_VARCHAR("{column}")) AS MAX_VALUE
        FROM {database}.{schema}."{table}"
    '''

    result = pd.read_sql(query, sf_conn)
    result.columns = [col.upper() for col in result.columns]

    row_count = int(result["ROW_COUNT"].iloc[0] or 0)
    non_null_count = int(result["NON_NULL_COUNT"].iloc[0] or 0)
    null_count = int(result["NULL_COUNT"].iloc[0] or 0)
    distinct_count = int(result["DISTINCT_COUNT"].iloc[0] or 0)

    distinct_ratio = distinct_count / row_count if row_count else 0
    non_null_ratio = non_null_count / row_count if row_count else 0

    return {
        "PROFILE_RUN_ID": profile_run_id,
        "SOURCE_SYSTEM": source_system,
        "TARGET_DATABASE": database,
        "TARGET_SCHEMA": schema,
        "TARGET_TABLE": table,
        "TARGET_COLUMN": column,
        "DATA_TYPE": data_type,
        "ROW_COUNT": row_count,
        "NON_NULL_COUNT": non_null_count,
        "NULL_COUNT": null_count,
        "DISTINCT_COUNT": distinct_count,
        "DISTINCT_RATIO": distinct_ratio,
        "NON_NULL_RATIO": non_null_ratio,
        "MIN_VALUE": clean_sql_value(result["MIN_VALUE"].iloc[0]),
        "MAX_VALUE": clean_sql_value(result["MAX_VALUE"].iloc[0]),
        "PROFILED_AT": datetime.now(UTC),
    }


def write_column_profiles(sf_conn, rows):
    if not rows:
        return

    df = pd.DataFrame(rows)

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="COLUMN_PROFILE",
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
        use_logical_type=True,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-system", required=False)
    parser.add_argument("--limit", type=int, required=False)
    args = parser.parse_args()

    profile_run_id = str(uuid4())
    sf_conn = get_snowflake_connection(
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
    )

    try:
        columns = get_columns_to_profile(sf_conn, args.source_system)

        if args.limit:
            columns = columns.head(args.limit)

        print(f"Profile run ID: {profile_run_id}")
        print(f"Columns to profile: {len(columns):,}")

        profile_rows = []

        for idx, row in columns.iterrows():
            print(
                f"[{idx + 1}/{len(columns)}] "
                f"{row['SOURCE_SYSTEM']}.{row['TARGET_TABLE']}.{row['TARGET_COLUMN']}"
            )

            try:
                profile_rows.append(profile_column(sf_conn, profile_run_id, row))
            except Exception as error:
                print(f"  ✗ Failed: {error}")

        write_column_profiles(sf_conn, profile_rows)
        print(f"Inserted {len(profile_rows):,} column profile row(s).")

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()