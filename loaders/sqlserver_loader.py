from datetime import UTC, datetime

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def normalize_chunk_for_load(
    chunk: pd.DataFrame,
    column_metadata: pd.DataFrame,
) -> pd.DataFrame:
    rename_map = dict(
        zip(column_metadata["COLUMN_NAME"], column_metadata["SNOWFLAKE_COLUMN_NAME"])
    )
    type_map = dict(
        zip(column_metadata["SNOWFLAKE_COLUMN_NAME"], column_metadata["SNOWFLAKE_DATA_TYPE"])
    )

    chunk = chunk.rename(columns=rename_map)

    for col in chunk.columns:
        sf_type = type_map.get(col, "VARCHAR")

        if sf_type.startswith("TIMESTAMP"):
            chunk[col] = pd.to_datetime(chunk[col], errors="coerce")
        elif sf_type == "DATE":
            chunk[col] = pd.to_datetime(chunk[col], errors="coerce").dt.date
        elif sf_type == "BOOLEAN":
            chunk[col] = chunk[col].astype("boolean")
        elif sf_type.startswith("NUMBER") or sf_type == "FLOAT":
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
        else:
            chunk[col] = chunk[col].astype("string")
            chunk[col] = chunk[col].replace(r"^\s*$", pd.NA, regex=True)

    return chunk


def load_sqlserver_table_to_snowflake(
    sql_conn,
    sf_conn,
    source_system: str,
    source_database: str,
    source_schema: str,
    source_table: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    column_metadata: pd.DataFrame,
    chunk_size: int,
) -> int:
    total_loaded = 0
    query = f"SELECT * FROM [{source_schema}].[{source_table}]"

    for chunk in pd.read_sql(query, sql_conn, chunksize=chunk_size):
        chunk = normalize_chunk_for_load(chunk, column_metadata)

        chunk["_SOURCE_SYSTEM"] = source_system
        chunk["_SOURCE_DATABASE"] = source_database
        chunk["_SOURCE_SCHEMA"] = source_schema
        chunk["_SOURCE_TABLE"] = source_table
        chunk["_INGESTED_AT"] = datetime.now(UTC)

        success, nchunks, nrows, output = write_pandas(
            conn=sf_conn,
            df=chunk,
            table_name=target_table,
            database=target_database,
            schema=target_schema,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=True,
            use_logical_type=True,
        )

        if not success:
            raise RuntimeError(f"Snowflake load failed for {target_table}: {output}")

        total_loaded += nrows
        print(f"Loaded chunk rows: {nrows}; total loaded so far: {total_loaded}")

    return total_loaded