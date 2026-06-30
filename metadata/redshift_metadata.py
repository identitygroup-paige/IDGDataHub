import pandas as pd


def get_source_tables(source_conn, source_schema, include_tables=None):
    include_tables = include_tables or []

    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """

    df = pd.read_sql(query, source_conn, params=[source_schema])
    df.columns = [col.upper() for col in df.columns]

    tables = df["TABLE_NAME"].tolist()

    if include_tables:
        include_lower = {table.lower() for table in include_tables}
        tables = [table for table in tables if table.lower() in include_lower]

    return tables


def get_column_metadata(
    source_conn,
    source_schema,
    table_name,
    clean_identifier,
    type_mapper,
):
    query = """
        SELECT
            column_name,
            ordinal_position,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """

    df = pd.read_sql(query, source_conn, params=[source_schema, table_name])
    df.columns = [col.upper() for col in df.columns]

    df["SNOWFLAKE_COLUMN_NAME"] = df["COLUMN_NAME"].apply(clean_identifier)
    df["SNOWFLAKE_DATA_TYPE"] = df.apply(type_mapper, axis=1)

    return df