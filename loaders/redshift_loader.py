from loaders.sqlserver_loader import load_sqlserver_table_to_snowflake


def load_redshift_table_to_snowflake(
    redshift_conn,
    sf_conn,
    source_system,
    source_database,
    source_schema,
    source_table,
    target_database,
    target_schema,
    target_table,
    column_metadata,
    chunk_size,
    query=None,
    query_params=None,
):
    return load_sqlserver_table_to_snowflake(
        sql_conn=redshift_conn,
        sf_conn=sf_conn,
        source_system=source_system,
        source_database=source_database,
        source_schema=source_schema,
        source_table=source_table,
        target_database=target_database,
        target_schema=target_schema,
        target_table=target_table,
        column_metadata=column_metadata,
        chunk_size=chunk_size,
        query=query,
        query_params=query_params,
    )