from datetime import UTC, datetime

import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from connectors.snowflake import get_snowflake_connection

load_dotenv()


def get_usable_relationships(sf_conn):
    query = """
        SELECT
            SOURCE_SYSTEM,
            PARENT_TABLE,
            PARENT_COLUMN,
            CHILD_TABLE,
            CHILD_COLUMN,
            CONFIDENCE_LABEL,
            MATCH_RATE
        FROM DATA_VALIDATION.INTELLIGENCE.V_RELATIONSHIP_CANDIDATES_USABLE
    """
    df = pd.read_sql(query, sf_conn)
    df.columns = [col.upper() for col in df.columns]
    return df


def infer_entity_name(table_name):
    table = table_name.upper()

    if "INVOICE" in table:
        return "INVOICE"
    if "ORDER" in table:
        return "ORDER"
    if "ESTIMATE" in table:
        return "ESTIMATE"
    if "CONTACT" in table:
        return "CONTACT"
    if "CUSTOMER" in table:
        return "CUSTOMER"
    if "CAMPUS" in table:
        return "CAMPUS"

    return table.replace("_QAD", "").replace("_E4", "").replace("_ENDEAV", "")


def build_graph_rows(relationships):
    now = datetime.now(UTC)
    graph_rows = []

    for _, row in relationships.iterrows():
        graph_rows.append(
            {
                "SOURCE_SYSTEM": row["SOURCE_SYSTEM"],
                "FROM_TABLE": row["PARENT_TABLE"],
                "FROM_COLUMN": row["PARENT_COLUMN"],
                "TO_TABLE": row["CHILD_TABLE"],
                "TO_COLUMN": row["CHILD_COLUMN"],
                "RELATIONSHIP_TYPE": "PARENT_CHILD",
                "CONFIDENCE_LABEL": row["CONFIDENCE_LABEL"],
                "MATCH_RATE": row["MATCH_RATE"],
                "CREATED_AT": now,
            }
        )

    return graph_rows


def build_entity_rows(relationships):
    now = datetime.now(UTC)
    rows = []
    seen = set()

    for _, row in relationships.iterrows():
        for table, role in [
            (row["PARENT_TABLE"], "PARENT"),
            (row["CHILD_TABLE"], "CHILD"),
        ]:
            key = (row["SOURCE_SYSTEM"], table, role)

            if key in seen:
                continue

            seen.add(key)

            rows.append(
                {
                    "ENTITY_NAME": infer_entity_name(table),
                    "SOURCE_SYSTEM": row["SOURCE_SYSTEM"],
                    "TARGET_TABLE": table,
                    "ENTITY_ROLE": role,
                    "CONFIDENCE_LABEL": row["CONFIDENCE_LABEL"],
                    "CREATED_AT": now,
                }
            )

    return rows


def write_table(sf_conn, rows, table_name):
    if not rows:
        print(f"No rows to write to {table_name}.")
        return

    df = pd.DataFrame(rows)

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name=table_name,
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
        use_logical_type=True,
    )

    print(f"Inserted {len(df):,} row(s) into {table_name}.")


def main():
    sf_conn = get_snowflake_connection(
        database="DATA_VALIDATION",
        schema="INTELLIGENCE",
    )

    try:
        relationships = get_usable_relationships(sf_conn)
        print(f"Usable relationships loaded: {len(relationships):,}")

        graph_rows = build_graph_rows(relationships)
        entity_rows = build_entity_rows(relationships)

        write_table(sf_conn, graph_rows, "RELATIONSHIP_GRAPH")
        write_table(sf_conn, entity_rows, "ENTITY_MAP")

    finally:
        sf_conn.close()


if __name__ == "__main__":
    main()