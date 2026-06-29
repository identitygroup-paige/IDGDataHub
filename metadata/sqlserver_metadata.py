import pandas as pd


def get_source_tables(sql_conn, source_schema: str, include_tables: list[str]) -> list[str]:
    if include_tables:
        return include_tables

    query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME;
    """

    df = pd.read_sql(query, sql_conn, params=[source_schema])
    return df["TABLE_NAME"].tolist()


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