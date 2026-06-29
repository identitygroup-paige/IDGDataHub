from connectors.sqlserver import get_sqlserver_connection

from ddl.snowflake_ddl import (
    clean_identifier,
    sqlserver_to_snowflake_type,
)

from metadata.sqlserver_metadata import (
    get_source_tables,
    get_column_metadata,
)

from loaders.sqlserver_loader import (
    load_sqlserver_table_to_snowflake,
)


def get_connection(source_config):
    return get_sqlserver_connection(source_config["env_prefix"])


def discover_tables(source_conn, source_config, load_config):
    return get_source_tables(
        sql_conn=source_conn,
        source_schema=source_config["schema"],
        include_tables=load_config.get("include_tables", []),
    )


def get_metadata(source_conn, source_config, table_name):
    return get_column_metadata(
        sql_conn=source_conn,
        source_schema=source_config["schema"],
        table_name=table_name,
        clean_identifier=clean_identifier,
        type_mapper=sqlserver_to_snowflake_type,
    )


def get_target_table_name(source_table, target_config):
    return (
        f"{clean_identifier(source_table)}"
        f"{target_config['table_suffix']}"
    )


def load_table(
    source_conn,
    sf_conn,
    source_config,
    target_config,
    source_table,
    target_table,
    column_metadata,
    chunk_size,
):
    return load_sqlserver_table_to_snowflake(
        sql_conn=source_conn,
        sf_conn=sf_conn,
        source_system=source_config["name"],
        source_database=source_config["database"],
        source_schema=source_config["schema"],
        source_table=source_table,
        target_database=target_config["database"],
        target_schema=target_config["schema"],
        target_table=target_table,
        column_metadata=column_metadata,
        chunk_size=chunk_size,
    )