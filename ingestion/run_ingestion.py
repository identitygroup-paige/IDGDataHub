import argparse
from datetime import UTC, datetime
from time import perf_counter

import yaml
from dotenv import load_dotenv

from connectors.snowflake import get_snowflake_connection
from ddl.snowflake_ddl import execute_ddl, generate_create_table_sql
from metadata.ingestion_metadata import (
    finish_run_log,
    get_snowflake_row_count,
    new_run_id,
    start_run_log,
    write_column_catalog,
    write_table_catalog,
    write_validation_results,
)
from metadata.watermarks import get_watermark, update_watermark
from sources.registry import get_source_adapter


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
    metadata_config = config["metadata"]

    planning_config = config.get("planning", {})
    planning_enabled = planning_config.get("enabled", False)

    incremental_config = config.get("incremental", {})
    incremental_tables = incremental_config.get("tables", {})

    source_adapter = get_source_adapter(source["type"])

    run_id = new_run_id()
    table_catalog_rows = []
    column_catalog_rows = []
    validation_rows = []
    table_results = []

    print_header("IDGDataHub Ingestion Load")
    print(f"Run ID: {run_id}")
    print(f"Source: {source['name']} ({source['database']}.{source['schema']})")
    print(f"Target: {target['database']}.{target['schema']}")
    print(f"Mode: {load['mode']}")
    print(f"Chunk size: {load['chunk_size']:,}")
    print(f"Planning mode: {'enabled' if planning_enabled else 'disabled'}")

    source_conn = source_adapter.get_connection(source)
    sf_conn = get_snowflake_connection(
        database=target["database"],
        schema=target["schema"],
    )

    try:
        print_step(f"{source['type']} connection successful")
        print_step("Snowflake connection successful")

        start_run_log(
            sf_conn=sf_conn,
            run_id=run_id,
            source_system=source["name"],
            source_database=source["database"],
            source_schema=source["schema"],
            target_database=target["database"],
            target_schema=target["schema"],
            load_mode=load["mode"],
            metadata_database=metadata_config["database"],
            metadata_schema=metadata_config["schema"],
        )
        print_step("Metadata run log started")

        tables = source_adapter.discover_tables(
            source_conn=source_conn,
            source_config=source,
            load_config=load,
        )

        print_step(f"Found {len(tables)} source table(s)")

        for source_table in tables:
            table_start = perf_counter()
            target_table = source_adapter.get_target_table_name(source_table, target)

            print_header(f"Loading {source['schema']}.{source_table} → {target_table}")

            metadata = source_adapter.get_metadata(
                source_conn=source_conn,
                source_config=source,
                table_name=source_table,
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

            source_row_count = source_adapter.get_row_count(
                source_conn=source_conn,
                source_config=source,
                table_name=source_table,
            )
            print_step(f"Source row count: {source_row_count:,}")

            incremental_table_config = incremental_tables.get(source_table)
            existing_watermark = None

            load_plan = source_adapter.build_full_refresh_query(
                source_config=source,
                table_name=source_table,
            )

            if incremental_table_config:
                watermark_column = incremental_table_config["watermark_column"]

                existing_watermark = get_watermark(
                    sf_conn=sf_conn,
                    source_config=source,
                    target_config=target,
                    source_table=source_table,
                    target_table=target_table,
                    watermark_column=watermark_column,
                    metadata_database=metadata_config["database"],
                    metadata_schema=metadata_config["schema"],
                )

                if planning_enabled:
                    planned_incremental_rows = source_adapter.get_incremental_row_count(
                        source_conn=source_conn,
                        source_config=source,
                        table_name=source_table,
                        watermark_column=watermark_column,
                        watermark_value=existing_watermark,
                    )

                    print_step(
                        f"Planning mode: {source_table} would load "
                        f"{planned_incremental_rows:,} incremental row(s) "
                        f"using {watermark_column} > {existing_watermark}"
                    )

            print_step(f"Load strategy selected: {load_plan['strategy']}")

            if source_row_count == 0:
                loaded_rows = 0
                print_step("Source table is empty; skipping data load")
            else:
                loaded_rows = source_adapter.load_table(
                    source_conn=source_conn,
                    sf_conn=sf_conn,
                    source_config=source,
                    target_config=target,
                    source_table=source_table,
                    target_table=target_table,
                    column_metadata=metadata,
                    chunk_size=load["chunk_size"],
                    load_plan=load_plan,
                )

            elapsed = perf_counter() - table_start

            target_row_count = get_snowflake_row_count(
                sf_conn=sf_conn,
                target_database=target["database"],
                target_schema=target["schema"],
                target_table=target_table,
            )

            row_count_match = source_row_count == loaded_rows == target_row_count
            loaded_at = datetime.now(UTC)
            load_status = "LOADED" if source_row_count > 0 else "EMPTY"

            if incremental_table_config:
                watermark_column = incremental_table_config["watermark_column"]

                max_watermark = source_adapter.get_max_watermark(
                    source_conn=source_conn,
                    source_config=source,
                    table_name=source_table,
                    watermark_column=watermark_column,
                )

                if max_watermark is not None:
                    update_watermark(
                        sf_conn=sf_conn,
                        source_config=source,
                        target_config=target,
                        source_table=source_table,
                        target_table=target_table,
                        watermark_column=watermark_column,
                        watermark_value=max_watermark,
                        run_id=run_id,
                        metadata_database=metadata_config["database"],
                        metadata_schema=metadata_config["schema"],
                    )
                    print_step(f"Watermark updated: {watermark_column} = {max_watermark}")

            table_catalog_rows.append(
                {
                    "RUN_ID": run_id,
                    "SOURCE_SYSTEM": source["name"],
                    "SOURCE_DATABASE": source["database"],
                    "SOURCE_SCHEMA": source["schema"],
                    "SOURCE_TABLE": source_table,
                    "TARGET_DATABASE": target["database"],
                    "TARGET_SCHEMA": target["schema"],
                    "TARGET_TABLE": target_table,
                    "SOURCE_ROW_COUNT": source_row_count,
                    "TARGET_ROW_COUNT": target_row_count,
                    "ROW_COUNT_MATCH": row_count_match,
                    "LOAD_STATUS": load_status,
                    "LOAD_SECONDS": elapsed,
                    "LOADED_AT": loaded_at,
                }
            )

            validation_rows.append(
                {
                    "RUN_ID": run_id,
                    "SOURCE_SYSTEM": source["name"],
                    "TARGET_DATABASE": target["database"],
                    "TARGET_SCHEMA": target["schema"],
                    "TARGET_TABLE": target_table,
                    "VALIDATION_NAME": "SOURCE_LOADED_TARGET_ROW_COUNT_MATCH",
                    "VALIDATION_STATUS": "PASS" if row_count_match else "FAIL",
                    "VALIDATION_VALUE": (
                        f"source={source_row_count}; "
                        f"loaded={loaded_rows}; "
                        f"target={target_row_count}"
                    ),
                    "VALIDATED_AT": loaded_at,
                }
            )

            for _, row in metadata.iterrows():
                column_catalog_rows.append(
                    {
                        "RUN_ID": run_id,
                        "SOURCE_SYSTEM": source["name"],
                        "SOURCE_DATABASE": source["database"],
                        "SOURCE_SCHEMA": source["schema"],
                        "SOURCE_TABLE": source_table,
                        "SOURCE_COLUMN": row["COLUMN_NAME"],
                        "SOURCE_ORDINAL_POSITION": row["ORDINAL_POSITION"],
                        "SOURCE_DATA_TYPE": row["DATA_TYPE"],
                        "SOURCE_CHARACTER_MAXIMUM_LENGTH": row[
                            "CHARACTER_MAXIMUM_LENGTH"
                        ],
                        "SOURCE_NUMERIC_PRECISION": row["NUMERIC_PRECISION"],
                        "SOURCE_NUMERIC_SCALE": row["NUMERIC_SCALE"],
                        "SOURCE_IS_NULLABLE": row["IS_NULLABLE"],
                        "TARGET_DATABASE": target["database"],
                        "TARGET_SCHEMA": target["schema"],
                        "TARGET_TABLE": target_table,
                        "TARGET_COLUMN": row["SNOWFLAKE_COLUMN_NAME"],
                        "TARGET_DATA_TYPE": row["SNOWFLAKE_DATA_TYPE"],
                        "CATALOGED_AT": loaded_at,
                    }
                )

            table_results.append(
                (
                    source_table,
                    target_table,
                    source_row_count,
                    loaded_rows,
                    target_row_count,
                    elapsed,
                    row_count_match,
                    load_plan["strategy"],
                )
            )

            print_step(f"Loaded {loaded_rows:,} rows in {elapsed:,.1f} seconds")
            print_step(
                f"Validation {'passed' if row_count_match else 'failed'} "
                f"(source={source_row_count:,}; loaded={loaded_rows:,}; "
                f"target={target_row_count:,})"
            )

        write_table_catalog(
            sf_conn,
            table_catalog_rows,
            metadata_config["database"],
            metadata_config["schema"],
        )
        write_column_catalog(
            sf_conn,
            column_catalog_rows,
            metadata_config["database"],
            metadata_config["schema"],
        )
        write_validation_results(
            sf_conn,
            validation_rows,
            metadata_config["database"],
            metadata_config["schema"],
        )
        print_step("Metadata tables updated")

        finish_run_log(
            sf_conn=sf_conn,
            run_id=run_id,
            metadata_database=metadata_config["database"],
            metadata_schema=metadata_config["schema"],
            status="SUCCESS",
        )
        print_step("Metadata run log completed")

        print_header("Load Summary")
        for (
            source_table,
            target_table,
            source_row_count,
            loaded_rows,
            target_row_count,
            elapsed,
            row_count_match,
            strategy,
        ) in table_results:
            print(
                f"✓ {source_table} → {target_table}: "
                f"strategy={strategy}, "
                f"source={source_row_count:,}, "
                f"loaded={loaded_rows:,}, "
                f"target={target_row_count:,}, "
                f"status={'PASS' if row_count_match else 'FAIL'} "
                f"({elapsed:,.1f}s)"
            )

        total_elapsed = perf_counter() - run_start
        print(f"\nCompleted {len(table_results)} table(s) in {total_elapsed:,.1f} seconds")

    except Exception as error:
        finish_run_log(
            sf_conn=sf_conn,
            run_id=run_id,
            metadata_database=metadata_config["database"],
            metadata_schema=metadata_config["schema"],
            status="FAILED",
            error_message=str(error),
        )
        raise

    finally:
        source_conn.close()
        sf_conn.close()


if __name__ == "__main__":
    main()