from datetime import UTC, datetime

def normalize_watermark_value(value):
    if value is None:
        return None

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    return value

def get_watermark(
    sf_conn,
    source_config: dict,
    target_config: dict,
    source_table: str,
    target_table: str,
    watermark_column: str,
    metadata_database: str,
    metadata_schema: str,
):
    with sf_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT WATERMARK_VALUE
            FROM {metadata_database}.{metadata_schema}.INGESTION_WATERMARKS
            WHERE SOURCE_SYSTEM = %s
              AND SOURCE_DATABASE = %s
              AND SOURCE_SCHEMA = %s
              AND SOURCE_TABLE = %s
              AND TARGET_DATABASE = %s
              AND TARGET_SCHEMA = %s
              AND TARGET_TABLE = %s
              AND WATERMARK_COLUMN = %s
            """,
            (
                source_config["name"],
                source_config["database"],
                source_config["schema"],
                source_table,
                target_config["database"],
                target_config["schema"],
                target_table,
                watermark_column,
            ),
        )
        row = cur.fetchone()
        return row[0] if row else None


def update_watermark(
    sf_conn,
    source_config: dict,
    target_config: dict,
    source_table: str,
    target_table: str,
    watermark_column: str,
    watermark_value,
    run_id: str,
    metadata_database: str,
    metadata_schema: str,
) -> None:
    watermark_value = normalize_watermark_value(watermark_value)
    with sf_conn.cursor() as cur:
        cur.execute(
            f"""
            MERGE INTO {metadata_database}.{metadata_schema}.INGESTION_WATERMARKS t
            USING (
                SELECT
                    %s AS SOURCE_SYSTEM,
                    %s AS SOURCE_DATABASE,
                    %s AS SOURCE_SCHEMA,
                    %s AS SOURCE_TABLE,
                    %s AS TARGET_DATABASE,
                    %s AS TARGET_SCHEMA,
                    %s AS TARGET_TABLE,
                    %s AS WATERMARK_COLUMN,
                    %s AS WATERMARK_VALUE,
                    %s AS LAST_SUCCESSFUL_RUN_ID,
                    %s AS UPDATED_AT
            ) s
            ON t.SOURCE_SYSTEM = s.SOURCE_SYSTEM
               AND t.SOURCE_DATABASE = s.SOURCE_DATABASE
               AND t.SOURCE_SCHEMA = s.SOURCE_SCHEMA
               AND t.SOURCE_TABLE = s.SOURCE_TABLE
               AND t.TARGET_DATABASE = s.TARGET_DATABASE
               AND t.TARGET_SCHEMA = s.TARGET_SCHEMA
               AND t.TARGET_TABLE = s.TARGET_TABLE
               AND t.WATERMARK_COLUMN = s.WATERMARK_COLUMN
            WHEN MATCHED THEN UPDATE SET
                WATERMARK_VALUE = s.WATERMARK_VALUE,
                LAST_SUCCESSFUL_RUN_ID = s.LAST_SUCCESSFUL_RUN_ID,
                UPDATED_AT = s.UPDATED_AT
            WHEN NOT MATCHED THEN INSERT (
                SOURCE_SYSTEM,
                SOURCE_DATABASE,
                SOURCE_SCHEMA,
                SOURCE_TABLE,
                TARGET_DATABASE,
                TARGET_SCHEMA,
                TARGET_TABLE,
                WATERMARK_COLUMN,
                WATERMARK_VALUE,
                LAST_SUCCESSFUL_RUN_ID,
                UPDATED_AT
            )
            VALUES (
                s.SOURCE_SYSTEM,
                s.SOURCE_DATABASE,
                s.SOURCE_SCHEMA,
                s.SOURCE_TABLE,
                s.TARGET_DATABASE,
                s.TARGET_SCHEMA,
                s.TARGET_TABLE,
                s.WATERMARK_COLUMN,
                s.WATERMARK_VALUE,
                s.LAST_SUCCESSFUL_RUN_ID,
                s.UPDATED_AT
            )
            """,
            (
                source_config["name"],
                source_config["database"],
                source_config["schema"],
                source_table,
                target_config["database"],
                target_config["schema"],
                target_table,
                watermark_column,
                watermark_value,
                run_id,
                datetime.now(UTC),
            ),
        )