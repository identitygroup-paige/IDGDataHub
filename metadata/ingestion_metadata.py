from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def new_run_id() -> str:
    return str(uuid4())


def start_run_log(
    sf_conn,
    run_id: str,
    source_system: str,
    source_database: str,
    source_schema: str,
    target_database: str,
    target_schema: str,
    load_mode: str,
    metadata_database: str,
    metadata_schema: str,
) -> None:
    with sf_conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {metadata_database}.{metadata_schema}.INGESTION_RUN_LOG
            (
                RUN_ID,
                SOURCE_SYSTEM,
                SOURCE_DATABASE,
                SOURCE_SCHEMA,
                TARGET_DATABASE,
                TARGET_SCHEMA,
                LOAD_MODE,
                STARTED_AT,
                STATUS
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_id,
                source_system,
                source_database,
                source_schema,
                target_database,
                target_schema,
                load_mode,
                datetime.now(UTC),
                "RUNNING",
            ),
        )


def finish_run_log(
    sf_conn,
    run_id: str,
    metadata_database: str,
    metadata_schema: str,
    status: str = "SUCCESS",
    error_message: str | None = None,
) -> None:
    with sf_conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {metadata_database}.{metadata_schema}.INGESTION_RUN_LOG
            SET FINISHED_AT = %s,
                STATUS = %s,
                ERROR_MESSAGE = %s
            WHERE RUN_ID = %s
            """,
            (datetime.now(UTC), status, error_message, run_id),
        )


def get_snowflake_row_count(
    sf_conn,
    target_database: str,
    target_schema: str,
    target_table: str,
) -> int:
    with sf_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {target_database}.{target_schema}.{target_table}")
        return int(cur.fetchone()[0])


def write_table_catalog(
    sf_conn,
    rows: list[dict],
    metadata_database: str,
    metadata_schema: str,
) -> None:
    df = pd.DataFrame(rows)
    if df.empty:
        return

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="INGESTION_TABLE_CATALOG",
        database=metadata_database,
        schema=metadata_schema,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=True,
        use_logical_type=True,
    )


def write_column_catalog(
    sf_conn,
    rows: list[dict],
    metadata_database: str,
    metadata_schema: str,
) -> None:
    df = pd.DataFrame(rows)
    if df.empty:
        return

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="INGESTION_COLUMN_CATALOG",
        database=metadata_database,
        schema=metadata_schema,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=True,
        use_logical_type=True,
    )


def write_validation_results(
    sf_conn,
    rows: list[dict],
    metadata_database: str,
    metadata_schema: str,
) -> None:
    df = pd.DataFrame(rows)
    if df.empty:
        return

    write_pandas(
        conn=sf_conn,
        df=df,
        table_name="INGESTION_VALIDATION_RESULTS",
        database=metadata_database,
        schema=metadata_schema,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=True,
        use_logical_type=True,
    )