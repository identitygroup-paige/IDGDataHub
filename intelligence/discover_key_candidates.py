import argparse
from datetime import UTC, datetime

import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from connectors.snowflake import get_snowflake_connection

load_dotenv()


def get_latest_column_profiles(sf_conn, source_system=None):
    query = """
        WITH latest AS (
            SELECT PROFILE_RUN_ID
            FROM DATA_VALIDATION.INTELLIGENCE.COLUMN_PROFILE
            WHERE 1 = 1
    """

    params = []

    if source_system:
        query += " AND SOURCE_SYSTEM = %s"
        params.append(source_system)

    query += """
            QUALIFY ROW_NUMBER() OVER (
                ORDER BY PROFILED_AT DESC
            ) = 1
        )
        SELECT cp.*
        FROM DATA_VALIDATION.INTELLIGENCE.COLUMN_PROFILE cp
        JOIN latest l
          ON cp.PROFILE_RUN_ID = l.PROFILE_RUN_ID
        WHERE 1 = 1
    """

    if source_system:
        query += " AND cp.SOURCE_SYSTEM = %s"
        params.append(source_system)

    return pd.read_sql(query, sf_conn, params=params)


def score_key_candidate(row):
    column = str(row["TARGET_COLUMN"]).upper()
    distinct_ratio = float(row["DISTINCT_RATIO"] or 0)
    non_null_ratio = float(row["NON_NULL_RATIO"] or 0)
    row_count = int(row["ROW_COUNT"] or 0)

    if row_count == 0:
        return None

    reasons = []
    score = 0.0
    candidate_type = None

    pk_name_patterns = (
        column == "ID"
        or column.endswith("_ID")
        or column.endswith("ID")
        or column.endswith("_NUMBER")
        or column.endswith("NUMBER")
        or column.endswith("_CODE")
        or column.endswith("CODE")
        or column.endswith("_KEY")
        or column.endswith("KEY")
    )

    business_key_patterns = (
        column in {"CUSTOMER", "CUSTOMERID", "JOBNUMBER", "ORDERNUMBER", "INVOICENUMBER"}
        or column.endswith("NUMBER")
        or column.endswith("_NUMBER")
    )

    if distinct_ratio >= 0.999 and non_null_ratio >= 0.999:
        if pk_name_patterns:
            candidate_type = "PRIMARY_KEY"
            score += 0.80
            reasons.append("unique, non-null, and naming pattern indicates primary key")
        else:
            candidate_type = "UNIQUE_ATTRIBUTE"
            score += 0.60
            reasons.append("unique attribute but not likely primary key")

    elif column.endswith("ID") or column.endswith("_ID"):
        candidate_type = "FOREIGN_KEY"
        score += 0.45
        reasons.append("column name looks like an ID/reference")

    elif business_key_patterns:
        candidate_type = "BUSINESS_KEY"
        score += 0.55
        reasons.append("column name looks like a business key")

    if column in {"ID", "ROWID"}:
        score += 0.15
        reasons.append("generic ID-style column")

    if non_null_ratio >= 0.95:
        score += 0.10
        reasons.append("mostly non-null")

    if distinct_ratio < 0.95 and candidate_type == "PRIMARY_KEY":
        return None

    if not candidate_type:
        return None

    return {
        "CANDIDATE_TYPE": candidate_type,
        "CONFIDENCE_SCORE": min(score, 1.0),
        "REASON": "; ".join(reasons),
    }


def discover_candidates(profiles):
    rows = []

    for _, row in profiles.iterrows():
        scored = score_key_candidate(row)

        if not scored:
            continue

        rows.append(
            {
                "PROFILE_RUN_ID": row["PROFILE_RUN_ID"],
                "SOURCE_SYSTEM": row["SOURCE_SYSTEM"],
                "TARGET_DATABASE": row["TARGET_DATABASE"],
                "TARGET_SCHEMA": row["TARGET_SCHEMA"],
                "TARGET_TABLE": row["TARGET_TABLE"],
                "TARGET_COLUMN": row["TARGET_COLUMN"],
                "DATA_TYPE": row["DATA_TYPE"],
                "CANDIDATE_TYPE": scored["CANDIDATE_TYPE"],
                "CONFIDENCE_SCORE": scored["CONFIDENCE_SCORE"],
                "REASON": scored["REASON"],
                "CREATED_AT": datetime.now(UTC),
            }
        )

    return rows


def write_candidates(sf_conn, rows):
    if not rows:
        print("No key candidates discovered.")
        return

    df = pd.DataFrame(rows)

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="KEY_CANDIDATES",
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
        use_logical_type=True,
    )

    print(f"Inserted {len(df):,} key candidate row(s).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-system", required=False)
    args = parser.parse_args()

    sf_conn = get_snowflake_connection(
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
    )

    try:
        profiles = get_latest_column_profiles(sf_conn, args.source_system)
        profiles.columns = [col.upper() for col in profiles.columns]

        print(f"Profiles loaded: {len(profiles):,}")

        rows = discover_candidates(profiles)
        write_candidates(sf_conn, rows)

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()