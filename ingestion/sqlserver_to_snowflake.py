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
        tables = get_source_tables(
            sql_conn=sql_conn,
            source_schema=source["schema"],
            include_tables=load.get("include_tables", []),
        )

        print(f"Found {len(tables)} source tables.")

        sample_table = tables[0]
        sample_target_table = f"{clean_identifier(sample_table)}{target['table_suffix']}"

        metadata = get_column_metadata(
            sql_conn=sql_conn,
            source_schema=source["schema"],
            table_name=sample_table,
            clean_identifier=clean_identifier,
            type_mapper=sqlserver_to_snowflake_type,
        )

        ddl = generate_create_table_sql(
            target_database=target["database"],
            target_schema=target["schema"],
            target_table=sample_target_table,
            column_metadata=metadata,
        )

        execute_ddl(sf_conn, ddl)
        print(f"Created table: {target['database']}.{target['schema']}.{sample_target_table}")

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

        print(
            f"Loaded {loaded_rows} rows into "
            f"{target['database']}.{target['schema']}.{sample_target_table}"
        )

    finally:
        sql_conn.close()
        sf_conn.close()


if __name__ == "__main__":
    main()