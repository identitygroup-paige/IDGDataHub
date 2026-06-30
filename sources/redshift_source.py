import pandas as pd

from connectors.redshift import get_redshift_connection
from ddl.snowflake_ddl import clean_identifier, redshift_to_snowflake_type
from loaders.redshift_loader import load_redshift_table_to_snowflake
from metadata.redshift_metadata import get_column_metadata, get_source_tables


def get_connection(source_config):
    return get_redshift_connection(source_config["env_prefix"])


def discover_tables(source_conn, source_config, load_config):
    return get_source_tables(
        source_conn=source_conn,
        source_schema=source_config["schema"],
        include_tables=load_config.get("include_tables", []),
    )


def get_metadata(source_conn, source_config, table_name):
    return get_column_metadata(
        source_conn=source_conn,
        source_schema=source_config["schema"],
        table_name=table_name,
        clean_identifier=clean_identifier,
        type_mapper=redshift_to_snowflake_type,
    )


def get_target_table_name(source_table, target_config):
    return f"{clean_identifier(source_table)}{target_config['table_suffix']}"


def load_table(
    source_conn,
    sf_conn,
    source_config,
    target_config,
    source_table,
    target_table,
    column_metadata,
    chunk_size,
    load_plan=None,
):
    query = load_plan.get("query") if load_plan else None
    query_params = load_plan.get("params") if load_plan else None

    return load_redshift_table_to_snowflake(
        redshift_conn=source_conn,
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
        query=query,
        query_params=query_params,
    )


def get_row_count(source_conn, source_config, table_name):
    query = f"""
        SELECT COUNT(*) AS row_count
        FROM {source_config["schema"]}.{table_name}
    """

    df = pd.read_sql(query, source_conn)
    df.columns = [col.upper() for col in df.columns]

    return int(df["ROW_COUNT"].iloc[0])


def get_max_watermark(source_conn, source_config, table_name, watermark_column):
    query = f"""
        SELECT MAX({watermark_column}) AS max_watermark
        FROM {source_config["schema"]}.{table_name}
    """

    df = pd.read_sql(query, source_conn)
    df.columns = [col.upper() for col in df.columns]

    result = df["MAX_WATERMARK"].iloc[0]
    return None if pd.isna(result) else result


def build_full_refresh_query(source_config, table_name):
    return {
        "query": f"""
            SELECT *
            FROM {source_config["schema"]}.{table_name}
        """,
        "params": [],
        "strategy": "full_refresh",
    }


def build_incremental_query(
    source_config,
    table_name,
    watermark_column,
    watermark_value,
):
    if watermark_value is None:
        return build_full_refresh_query(source_config, table_name)

    return {
        "query": f"""
            SELECT *
            FROM {source_config["schema"]}.{table_name}
            WHERE {watermark_column} > %s
        """,
        "params": [watermark_value],
        "strategy": "timestamp",
    }


def get_incremental_row_count(
    source_conn,
    source_config,
    table_name,
    watermark_column,
    watermark_value,
):
    if watermark_value is None:
        return get_row_count(source_conn, source_config, table_name)

    query = f"""
        SELECT COUNT(*) AS row_count
        FROM {source_config["schema"]}.{table_name}
        WHERE {watermark_column} > %s
    """

    df = pd.read_sql(query, source_conn, params=[watermark_value])
    df.columns = [col.upper() for col in df.columns]

    return int(df["ROW_COUNT"].iloc[0])