import argparse
from datetime import UTC, datetime
from time import perf_counter

import yaml
from dotenv import load_dotenv

from connectors.snowflake import get_snowflake_connection
from ddl.snowflake_ddl import (
    execute_ddl,
    generate_create_table_if_not_exists_sql,
    generate_create_table_sql,
)
from loaders.snowflake_merge import build_merge_sql, execute_merge
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


def print_error(message: str) -> None:
    print(f"✗ {message}")


def build_load_plan(
    source_adapter,
    source: dict,
    load_mode: str,
    source_table: str,
    incremental_table_config: dict | None,
    watermark_column,
    existing_watermark,
):
    if load_mode == "full_refresh":
        return source_adapter.build_full_refresh_query(
            source_config=source,
            table_name=source_table,
        )

    if load_mode in ("incremental_plan", "incremental"):
        if incremental_table_config:
            return source_adapter.build_incremental_query(
                source_config=source,
                table_name=source_table,
                watermark_column=watermark_column,
                watermark_value=existing_watermark,
            )

        return source_adapter.build_full_refresh_query(
            source_config=source,
            table_name=source_table,
        )

    raise ValueError(f"Unknown load mode: {load_mode}")


def validate_row_counts(load_plan, source_row_count, loaded_rows, target_row_count):
    if load_plan["strategy"] == "full_refresh":
        row_count_match = source_row_count == loaded_rows == target_row_count
        validation_value = (
            f"source={source_row_count}; "
            f"loaded={loaded_rows}; "
            f"target={target_row_count}"
        )
    else:
        row_count_match = loaded_rows >= 0
        validation_value = (
            f"source_total={source_row_count}; "
            f"incremental_loaded={loaded_rows}; "
            f"target_after_merge={target_row_count}"
        )

    return row_count_match, validation_value


def append_failed_table_result(
    table_catalog_rows,
    validation_rows,
    table_results,
    run_id,
    source,
    target,
    source_table,
    target_table,
    table_start,
    table_error,
):
    elapsed = perf_counter() - table_start
    failed_at = datetime.now(UTC)
    error_message = str(table_error)

    print_error(f"Failed {source_table}: {error_message}")

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
            "SOURCE_ROW_COUNT": None,
            "TARGET_ROW_COUNT": None,
            "ROW_COUNT_MATCH": False,
            "LOAD_STATUS": "FAILED",
            "LOAD_SECONDS": elapsed,
            "LOADED_AT": failed_at,
        }
    )

    validation_rows.append(
        {
            "RUN_ID": run_id,
            "SOURCE_SYSTEM": source["name"],
            "TARGET_DATABASE": target["database"],
            "TARGET_SCHEMA": target["schema"],
            "TARGET_TABLE": target_table,
            "VALIDATION_NAME": "TABLE_LOAD_ERROR",
            "VALIDATION_STATUS": "FAIL",
            "VALIDATION_VALUE": error_message[:500],
            "VALIDATED_AT": failed_at,
        }
    )

    table_results.append(
        (
            source_table,
            target_table,
            0,
            0,
            0,
            elapsed,
            False,
            "error",
            "FAILED",
        )
    )


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

    load_mode = load["mode"]
    planning_enabled = config.get("planning", {}).get("enabled", False)
    incremental_tables = config.get("incremental", {}).get("tables", {})

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
    print(f"Mode: {load_mode}")
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
            load_mode=load_mode,
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
            stage_table = f"{target_table}_STG"

            try:
                print_header(
                    f"Loading {source['schema']}.{source_table} → {target_table}"
                )

                metadata = source_adapter.get_metadata(
                    source_conn=source_conn,
                    source_config=source,
                    table_name=source_table,
                )
                print_step(f"Metadata discovered ({len(metadata)} columns)")

                incremental_table_config = incremental_tables.get(source_table)
                primary_key = (
                    incremental_table_config.get("primary_key", [])
                    if incremental_table_config
                    else []
                )
                watermark_column = None
                existing_watermark = None
                planned_incremental_rows = None

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
                        planned_incremental_rows = (
                            source_adapter.get_incremental_row_count(
                                source_conn=source_conn,
                                source_config=source,
                                table_name=source_table,
                                watermark_column=watermark_column,
                                watermark_value=existing_watermark,
                            )
                        )
                        print_step(
                            f"Planning mode: {source_table} would load "
                            f"{planned_incremental_rows:,} incremental row(s) "
                            f"using {watermark_column} > {existing_watermark}"
                        )

                load_plan = build_load_plan(
                    source_adapter=source_adapter,
                    source=source,
                    load_mode=load_mode,
                    source_table=source_table,
                    incremental_table_config=incremental_table_config,
                    watermark_column=watermark_column,
                    existing_watermark=existing_watermark,
                )
                print_step(f"Load strategy selected: {load_plan['strategy']}")

                if load_plan["strategy"] == "full_refresh":
                    target_ddl = generate_create_table_sql(
                        target_database=target["database"],
                        target_schema=target["schema"],
                        target_table=target_table,
                        column_metadata=metadata,
                    )
                    execute_ddl(sf_conn, target_ddl)
                    print_step("Snowflake target table created/replaced")
                else:
                    target_ddl = generate_create_table_if_not_exists_sql(
                        target_database=target["database"],
                        target_schema=target["schema"],
                        target_table=target_table,
                        column_metadata=metadata,
                    )
                    execute_ddl(sf_conn, target_ddl)
                    print_step("Snowflake target table verified/created if missing")

                source_row_count = source_adapter.get_row_count(
                    source_conn=source_conn,
                    source_config=source,
                    table_name=source_table,
                )
                print_step(f"Source row count: {source_row_count:,}")

                use_merge = (
                    load_mode == "incremental"
                    and load_plan["strategy"] != "full_refresh"
                    and bool(primary_key)
                )
                load_target_table = stage_table if use_merge else target_table

                if use_merge:
                    stage_ddl = generate_create_table_sql(
                        target_database=target["database"],
                        target_schema=target["schema"],
                        target_table=stage_table,
                        column_metadata=metadata,
                    )
                    execute_ddl(sf_conn, stage_ddl)
                    print_step(f"Snowflake stage table created/replaced: {stage_table}")

                if (
                    load_mode == "incremental_plan"
                    and load_plan["strategy"] != "full_refresh"
                ):
                    loaded_rows = 0
                    print_step("Incremental plan mode: skipping table load")
                elif source_row_count == 0:
                    loaded_rows = 0
                    print_step("Source table is empty; skipping data load")
                else:
                    loaded_rows = source_adapter.load_table(
                        source_conn=source_conn,
                        sf_conn=sf_conn,
                        source_config=source,
                        target_config=target,
                        source_table=source_table,
                        target_table=load_target_table,
                        column_metadata=metadata,
                        chunk_size=load["chunk_size"],
                        load_plan=load_plan,
                    )

                if use_merge and loaded_rows > 0:
                    merge_sql = build_merge_sql(
                        target_database=target["database"],
                        target_schema=target["schema"],
                        target_table=target_table,
                        stage_table=stage_table,
                        column_metadata=metadata,
                        primary_key=primary_key,
                    )
                    execute_merge(sf_conn, merge_sql)
                    print_step(
                        f"Merged {stage_table} into {target_table} using {primary_key}"
                    )

                elapsed = perf_counter() - table_start

                target_row_count = get_snowflake_row_count(
                    sf_conn=sf_conn,
                    target_database=target["database"],
                    target_schema=target["schema"],
                    target_table=target_table,
                )

                if (
                    load_mode == "incremental_plan"
                    and load_plan["strategy"] != "full_refresh"
                ):
                    row_count_match = True
                    validation_value = (
                        f"incremental_plan_only=true; "
                        f"source_total={source_row_count}; "
                        f"planned_rows_not_loaded={planned_incremental_rows}"
                    )
                else:
                    row_count_match, validation_value = validate_row_counts(
                        load_plan=load_plan,
                        source_row_count=source_row_count,
                        loaded_rows=loaded_rows,
                        target_row_count=target_row_count,
                    )

                loaded_at = datetime.now(UTC)
                load_status = "PLANNED" if (
                    load_mode == "incremental_plan"
                    and load_plan["strategy"] != "full_refresh"
                ) else (
                    "MERGED"
                    if use_merge
                    else ("LOADED" if source_row_count > 0 else "EMPTY")
                )

                if incremental_table_config and load_mode != "incremental_plan":
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
                        print_step(
                            f"Watermark updated: {watermark_column} = {max_watermark}"
                        )

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
                        "VALIDATION_VALUE": validation_value,
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
                        load_status,
                    )
                )

                print_step(f"Loaded {loaded_rows:,} rows in {elapsed:,.1f} seconds")
                print_step(
                    f"Validation {'passed' if row_count_match else 'failed'} "
                    f"(source={source_row_count:,}; loaded={loaded_rows:,}; "
                    f"target={target_row_count:,})"
                )

            except Exception as table_error:
                append_failed_table_result(
                    table_catalog_rows=table_catalog_rows,
                    validation_rows=validation_rows,
                    table_results=table_results,
                    run_id=run_id,
                    source=source,
                    target=target,
                    source_table=source_table,
                    target_table=target_table,
                    table_start=table_start,
                    table_error=table_error,
                )
                continue

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

        failed_tables = [
            result for result in table_results if result[8] == "FAILED"
        ]

        finish_run_log(
            sf_conn=sf_conn,
            run_id=run_id,
            metadata_database=metadata_config["database"],
            metadata_schema=metadata_config["schema"],
            status="PARTIAL_SUCCESS" if failed_tables else "SUCCESS",
            error_message=(
                f"{len(failed_tables)} table(s) failed"
                if failed_tables
                else None
            ),
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
            load_status,
        ) in table_results:
            print(
                f"✓ {source_table} → {target_table}: "
                f"strategy={strategy}, "
                f"load_status={load_status}, "
                f"source={source_row_count:,}, "
                f"loaded={loaded_rows:,}, "
                f"target={target_row_count:,}, "
                f"status={'PASS' if row_count_match else 'FAIL'} "
                f"({elapsed:,.1f}s)"
            )

        total_elapsed = perf_counter() - run_start
        print(f"\nCompleted {len(table_results)} table(s) in {total_elapsed:,.1f} seconds")

        if failed_tables:
            print_error(f"{len(failed_tables)} table(s) failed. Check metadata validation logs.")

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