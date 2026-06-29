import argparse
from time import perf_counter

import yaml
from dotenv import load_dotenv

from connectors.snowflake import get_snowflake_connection
from connectors.sqlserver import get_sqlserver_connection
from ddl.snowflake_ddl import (
    clean_identifier,
    execute_ddl,
    generate_create_table_sql,
    sqlserver_to_snowflake_type,
)
from loaders.sqlserver_loader import load_sqlserver_table_to_snowflake
from metadata.sqlserver_metadata import get_column_metadata, get_source_tables


load_dotenv()


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def print_header(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(title)
    print(f"{'=' * 80}")


def print_step(message: str) -> None:
    print(f"✓ {message}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    run_start = perf_counter()
    config = load_config(args.config)

    source = config["source"]
    target = config["target"]
    load = config["load"]

    print_header("IDGDataHub SQL Server → Snowflake Load")
    print(f"Source: {source['name']} ({source['database']}.{source['schema']})")
    print(f"Target: {target['database']}.{target['schema']}")
    print(f"Mode: {load['mode']}")
    print(f"Chunk size: {load['chunk_size']:,}")

    sql_conn = get_sqlserver_connection(source["env_prefix"])
    sf_conn = get_snowflake_connection(
        database=target["database"],
        schema=target["schema"],
    )

    try:
        print_step("SQL Server connection successful")
        print_step("Snowflake connection successful")

        tables = get_source_tables(
            sql_conn=sql_conn,
            source_schema=source["schema"],
            include_tables=load.get("include_tables", []),
        )

        print_step(f"Found {len(tables)} source table(s)")

        table_results = []

        for source_table in tables:
            table_start = perf_counter()
            target_table = f"{clean_identifier(source_table)}{target['table_suffix']}"

            print_header(f"Loading {source['schema']}.{source_table} → {target_table}")

            metadata = get_column_metadata(
                sql_conn=sql_conn,
                source_schema=source["schema"],
                table_name=source_table,
                clean_identifier=clean_identifier,
                type_mapper=sqlserver_to_snowflake_type,
            )
            print_step(f"Metadata discovered ({len(metadata)} columns)")

            ddl = generate_create_table_sql(
                target_database=target["database"],
                target_schema=target["schema"],
                target_table=target_table,
                column_metadata=metadata,
            )

            execute_ddl(sf_conn, ddl)
            print_step("Snowflake table created/replaced")

            loaded_rows = load_sqlserver_table_to_snowflake(
                sql_conn=sql_conn,
                sf_conn=sf_conn,
                source_system=source["name"],
                source_database=source["database"],
                source_schema=source["schema"],
                source_table=source_table,
                target_database=target["database"],
                target_schema=target["schema"],
                target_table=target_table,
                column_metadata=metadata,
                chunk_size=load["chunk_size"],
            )

            elapsed = perf_counter() - table_start
            table_results.append((source_table, target_table, loaded_rows, elapsed))

            print_step(f"Loaded {loaded_rows:,} rows in {elapsed:,.1f} seconds")

        print_header("Load Summary")
        for source_table, target_table, loaded_rows, elapsed in table_results:
            print(f"✓ {source_table} → {target_table}: {loaded_rows:,} rows ({elapsed:,.1f}s)")

        total_elapsed = perf_counter() - run_start
        print(f"\nCompleted {len(table_results)} table(s) in {total_elapsed:,.1f} seconds")

    finally:
        sql_conn.close()
        sf_conn.close()


if __name__ == "__main__":
    main()