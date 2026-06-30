import pandas as pd


def get_source_tables(
    sql_conn,
    source_schema: str,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
) -> list[str]:
    """
    Returns the list of source tables.

    Behavior:
      • If include_tables is supplied, only those tables are returned.
      • Otherwise all base tables in the schema are returned.
      • Any tables listed in exclude_tables are removed.
    """

    include_tables = include_tables or []
    exclude_tables = exclude_tables or []

    if include_tables:
        tables = include_tables.copy()
    else:
        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME;
        """

        df = pd.read_sql(query, sql_conn, params=[source_schema])
        tables = df["TABLE_NAME"].tolist()

    if exclude_tables:
        exclude_lookup = {t.lower() for t in exclude_tables}
        tables = [
            table
            for table in tables
            if table.lower() not in exclude_lookup
        ]

    return tables

def get_column_metadata(sql_conn, source_schema: str, table_name: str, clean_identifier, type_mapper) -> pd.DataFrame:
    query = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION;
    """

    df = pd.read_sql(query, sql_conn, params=[source_schema, table_name])
    df["SNOWFLAKE_COLUMN_NAME"] = df["COLUMN_NAME"].apply(clean_identifier)
    df["SNOWFLAKE_DATA_TYPE"] = df.apply(type_mapper, axis=1)

    return df