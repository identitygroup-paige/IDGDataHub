def build_merge_sql(
    target_database: str,
    target_schema: str,
    target_table: str,
    stage_table: str,
    column_metadata,
    primary_key: list[str],
) -> str:
    target_fqn = f'{target_database}.{target_schema}."{target_table}"'
    stage_fqn = f'{target_database}.{target_schema}."{stage_table}"'

    columns = list(column_metadata["SNOWFLAKE_COLUMN_NAME"]) + [
        "_SOURCE_SYSTEM",
        "_SOURCE_DATABASE",
        "_SOURCE_SCHEMA",
        "_SOURCE_TABLE",
        "_INGESTED_AT",
    ]

    pk_cols = [col.upper() for col in primary_key]

    on_clause = " AND ".join(
        [f't."{col}" = s."{col}"' for col in pk_cols]
    )

    update_cols = [col for col in columns if col not in pk_cols]

    update_clause = ",\n        ".join(
        [f't."{col}" = s."{col}"' for col in update_cols]
    )

    insert_cols = ", ".join([f'"{col}"' for col in columns])
    insert_values = ", ".join([f's."{col}"' for col in columns])

    return f"""
MERGE INTO {target_fqn} t
USING {stage_fqn} s
    ON {on_clause}
WHEN MATCHED THEN UPDATE SET
        {update_clause}
WHEN NOT MATCHED THEN INSERT ({insert_cols})
VALUES ({insert_values});
"""


def execute_merge(sf_conn, merge_sql: str) -> None:
    with sf_conn.cursor() as cur:
        cur.execute(merge_sql)