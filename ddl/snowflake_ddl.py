import re

import pandas as pd


SNOWFLAKE_RESERVED_WORDS = {
    "START", "END", "TEXT", "DATE", "TIME", "TIMESTAMP", "USER",
    "CURRENT", "GROUP", "ORDER", "BY", "SELECT", "FROM", "WHERE",
    "TABLE", "COLUMN", "VALUE", "VALUES", "TYPE", "LEVEL", "COPY",
}


def clean_identifier(name: str) -> str:
    name = str(name).upper()
    name = re.sub(r"[^A-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")

    if not name:
        name = "UNNAMED_COL"

    if name[0].isdigit():
        name = f"COL_{name}"

    if name in SNOWFLAKE_RESERVED_WORDS:
        name = f"{name}_COL"

    return name


def sqlserver_to_snowflake_type(row) -> str:
    data_type = str(row["DATA_TYPE"]).lower()
    max_len = row["CHARACTER_MAXIMUM_LENGTH"]
    precision = row["NUMERIC_PRECISION"]
    scale = row["NUMERIC_SCALE"]

    if data_type in {"bigint", "int", "smallint", "tinyint"}:
        return "NUMBER(38,0)"

    if data_type == "bit":
        return "BOOLEAN"

    if data_type in {"decimal", "numeric", "money", "smallmoney"}:
        p = int(precision) if pd.notna(precision) else 38
        s = int(scale) if pd.notna(scale) else 4
        return f"NUMBER({min(p, 38)},{s})"

    if data_type in {"float", "real"}:
        return "FLOAT"

    if data_type == "date":
        return "DATE"

    if data_type in {"datetime", "datetime2", "smalldatetime"}:
        return "TIMESTAMP_NTZ"

    if data_type == "datetimeoffset":
        return "TIMESTAMP_TZ"

    if data_type == "time":
        return "TIME"

    if data_type == "uniqueidentifier":
        return "VARCHAR(36)"

    if data_type in {"binary", "varbinary", "image"}:
        return "BINARY"

    if data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext", "xml"}:
        if pd.isna(max_len) or int(max_len) < 0:
            return "VARCHAR"
        return f"VARCHAR({min(int(max_len), 16777216)})"

    return "VARCHAR"


def generate_create_table_sql(
    target_database: str,
    target_schema: str,
    target_table: str,
    column_metadata: pd.DataFrame,
) -> str:
    column_defs = []

    for _, row in column_metadata.iterrows():
        column_defs.append(
            f'"{row["SNOWFLAKE_COLUMN_NAME"]}" {row["SNOWFLAKE_DATA_TYPE"]}'
        )

    lineage_cols = [
        '"_SOURCE_SYSTEM" VARCHAR',
        '"_SOURCE_DATABASE" VARCHAR',
        '"_SOURCE_SCHEMA" VARCHAR',
        '"_SOURCE_TABLE" VARCHAR',
        '"_INGESTED_AT" TIMESTAMP_TZ',
    ]

    return f"""
CREATE OR REPLACE TABLE {target_database}.{target_schema}.{target_table} (
    {", ".join(column_defs + lineage_cols)}
);
""".strip()


def execute_ddl(sf_conn, ddl: str) -> None:
    with sf_conn.cursor() as cur:
        cur.execute(ddl)