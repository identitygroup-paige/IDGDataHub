import argparse
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

from connectors.sqlserver import get_sqlserver_connection
from connectors.snowflake import get_snowflake_connection

from datetime import datetime, UTC

from snowflake.connector.pandas_tools import write_pandas
from loaders.sqlserver_loader import load_sqlserver_table_to_snowflake
from metadata.sqlserver_metadata import get_column_metadata, get_source_tables


from ddl.snowflake_ddl import (
    clean_identifier,
    execute_ddl,
    generate_create_table_sql,
    sqlserver_to_snowflake_type,
)

load_dotenv()


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


import re


SNOWFLAKE_RESERVED_WORDS = {
    "START", "END", "TEXT", "DATE", "TIME", "TIMESTAMP", "USER",
    "CURRENT", "GROUP", "ORDER", "BY", "SELECT", "FROM", "WHERE",
    "TABLE", "COLUMN", "VALUE", "VALUES", "TYPE", "LEVEL", "COPY",
}


# def clean_identifier(name: str) -> str:
#     name = str(name).upper()
#     name = re.sub(r"[^A-Z0-9_]", "_", name)
#     name = re.sub(r"_+", "_", name).strip("_")

#     if not name:
#         name = "UNNAMED_COL"

#     if name[0].isdigit():
#         name = f"COL_{name}"

#     if name in SNOWFLAKE_RESERVED_WORDS:
#         name = f"{name}_COL"

#     return name

# def normalize_chunk_for_load(chunk: pd.DataFrame, column_metadata: pd.DataFrame) -> pd.DataFrame:
#     rename_map = dict(
#         zip(column_metadata["COLUMN_NAME"], column_metadata["SNOWFLAKE_COLUMN_NAME"])
#     )
#     type_map = dict(
#         zip(column_metadata["SNOWFLAKE_COLUMN_NAME"], column_metadata["SNOWFLAKE_DATA_TYPE"])
#     )

#     chunk = chunk.rename(columns=rename_map)

#     for col in chunk.columns:
#         sf_type = type_map.get(col, "VARCHAR")

#         if sf_type.startswith("TIMESTAMP"):
#             chunk[col] = pd.to_datetime(chunk[col], errors="coerce")
#         elif sf_type == "DATE":
#             chunk[col] = pd.to_datetime(chunk[col], errors="coerce").dt.date
#         elif sf_type == "BOOLEAN":
#             chunk[col] = chunk[col].astype("boolean")
#         elif sf_type.startswith("NUMBER") or sf_type == "FLOAT":
#             chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
#         else:
#             chunk[col] = chunk[col].astype("string")
#             chunk[col] = chunk[col].replace(r"^\s*$", pd.NA, regex=True)

#     return chunk


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

# def sqlserver_to_snowflake_type(row) -> str:
#     data_type = str(row["DATA_TYPE"]).lower()
#     max_len = row["CHARACTER_MAXIMUM_LENGTH"]
#     precision = row["NUMERIC_PRECISION"]
#     scale = row["NUMERIC_SCALE"]

#     if data_type in {"bigint", "int", "smallint", "tinyint"}:
#         return "NUMBER(38,0)"

#     if data_type == "bit":
#         return "BOOLEAN"

#     if data_type in {"decimal", "numeric", "money", "smallmoney"}:
#         p = int(precision) if pd.notna(precision) else 38
#         s = int(scale) if pd.notna(scale) else 4
#         return f"NUMBER({min(p, 38)},{s})"

#     if data_type in {"float", "real"}:
#         return "FLOAT"

#     if data_type == "date":
#         return "DATE"

#     if data_type in {"datetime", "datetime2", "smalldatetime"}:
#         return "TIMESTAMP_NTZ"

#     if data_type == "datetimeoffset":
#         return "TIMESTAMP_TZ"

#     if data_type == "time":
#         return "TIME"

#     if data_type == "uniqueidentifier":
#         return "VARCHAR(36)"

#     if data_type in {"binary", "varbinary", "image"}:
#         return "BINARY"

#     if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext", "xml"}:
#         if pd.isna(max_len) or int(max_len) < 0:
#             return "VARCHAR"
#         return f"VARCHAR({min(int(max_len), 16777216)})"

#     return "VARCHAR"


# def get_column_metadata(sql_conn, source_schema: str, table_name: str) -> pd.DataFrame:
#     query = """
#         SELECT
#             TABLE_SCHEMA,
#             TABLE_NAME,
#             COLUMN_NAME,
#             ORDINAL_POSITION,
#             DATA_TYPE,
#             CHARACTER_MAXIMUM_LENGTH,
#             NUMERIC_PRECISION,
#             NUMERIC_SCALE,
#             IS_NULLABLE
#         FROM INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_SCHEMA = ?
#           AND TABLE_NAME = ?
#         ORDER BY ORDINAL_POSITION;
#     """

#     df = pd.read_sql(query, sql_conn, params=[source_schema, table_name])
#     df["SNOWFLAKE_COLUMN_NAME"] = df["COLUMN_NAME"].apply(clean_identifier)
#     df["SNOWFLAKE_DATA_TYPE"] = df.apply(sqlserver_to_snowflake_type, axis=1)

#     return df


# def generate_create_table_sql(
#     target_database: str,
#     target_schema: str,
#     target_table: str,
#     column_metadata: pd.DataFrame,
# ) -> str:
#     column_defs = []

#     for _, row in column_metadata.iterrows():
#         col_name = row["SNOWFLAKE_COLUMN_NAME"]
#         col_type = row["SNOWFLAKE_DATA_TYPE"]
#         column_defs.append(f'"{col_name}" {col_type}')

#     lineage_cols = [
#         '"_SOURCE_SYSTEM" VARCHAR',
#         '"_SOURCE_DATABASE" VARCHAR',
#         '"_SOURCE_SCHEMA" VARCHAR',
#         '"_SOURCE_TABLE" VARCHAR',
#         '"_INGESTED_AT" TIMESTAMP_TZ',
#     ]

#     all_cols = column_defs + lineage_cols

#     return f"""
# CREATE OR REPLACE TABLE {target_database}.{target_schema}.{target_table} (
#     {", ".join(all_cols)}
# );
# """.strip()

# def execute_ddl(sf_conn, ddl: str) -> None:
#     with sf_conn.cursor() as cur:
#         cur.execute(ddl)

# def get_source_tables(sql_conn, source_schema: str, include_tables: list[str]) -> list[str]:
#     if include_tables:
#         return include_tables

#     query = """
#         SELECT TABLE_NAME
#         FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_SCHEMA = ?
#           AND TABLE_TYPE = 'BASE TABLE'
#         ORDER BY TABLE_NAME;
#     """

#     df = pd.read_sql(query, sql_conn, params=[source_schema])
#     return df["TABLE_NAME"].tolist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    source = config["source"]
    target = config["target"]
    load = config["load"]

    print("Loaded config.")
    print(f"Source: {source['name']}")
    print(f"Target: {target['database']}.{target['schema']}")

    sql_conn = get_sqlserver_connection(source["env_prefix"])
    sf_conn = get_snowflake_connection(
        database=target["database"],
        schema=target["schema"],
    )

    try:
        print("SQL Server connection successful.")
        print("Snowflake connection successful.")

        tables = get_source_tables(
            sql_conn=sql_conn,
            source_schema=source["schema"],
            include_tables=load.get("include_tables", []),
        )

        print(f"Found {len(tables)} source tables.")
        for table in tables[:20]:
            print(f" - {table}")

        if len(tables) > 20:
            print(f"... and {len(tables) - 20} more.")

        sample_table = tables[0]
        sample_target_table = f"{clean_identifier(sample_table)}{target['table_suffix']}"

        metadata = get_column_metadata(
        sql_conn=sql_conn,
        source_schema=source["schema"],
        table_name=sample_table,
        clean_identifier=clean_identifier,
        type_mapper=sqlserver_to_snowflake_type,
    )

        print(f"\nSample metadata for {sample_table}:")
        print(
            metadata[
                ["COLUMN_NAME", "DATA_TYPE", "SNOWFLAKE_COLUMN_NAME", "SNOWFLAKE_DATA_TYPE"]
            ].head()
        )

        print("\nGenerated Snowflake DDL:")
        print(
            generate_create_table_sql(
                target_database=target["database"],
                target_schema=target["schema"],
                target_table=sample_target_table,
                column_metadata=metadata,
            )
        )
        ddl = ddl = generate_create_table_sql(
    target_database=target["database"],
    target_schema=target["schema"],
    target_table=sample_target_table,
    column_metadata=metadata,
)

        print("\nGenerated Snowflake DDL:")
        print(ddl)

        execute_ddl(sf_conn, ddl)
        print(f"\nCreated table: {target['database']}.{target['schema']}.{sample_target_table}")

        loaded_rows = load_sqlserver_table_to_snowflake(
            sql_conn=sql_conn,
            sf_conn=sf_conn,
            source_system=source["name"],
            source_database=source["database"],
            source_schema=source["schema"],
            source_table=sample_table,
            target_database=target["database"],
            target_schema=target["schema"],
            target_table=sample_target_table,
            column_metadata=metadata,
            chunk_size=load["chunk_size"],
        )

        print(f"\nLoaded {loaded_rows} rows into {target['database']}.{target['schema']}.{sample_target_table}")

    finally:
        sql_conn.close()
        sf_conn.close()


if __name__ == "__main__":
    main()