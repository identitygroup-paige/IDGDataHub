from connectors.redshift import get_redshift_connection
from ddl.snowflake_ddl import clean_identifier, redshift_to_snowflake_type
from metadata.redshift_metadata import get_column_metadata, get_source_tables

conn = get_redshift_connection("REDSHIFT")

tables = get_source_tables(conn, "public")
print(f"Found {len(tables)} Redshift table(s)")
print(tables[:20])

if tables:
    metadata = get_column_metadata(
        source_conn=conn,
        source_schema="public",
        table_name=tables[0],
        clean_identifier=clean_identifier,
        type_mapper=redshift_to_snowflake_type,
    )
    print(metadata.head())

conn.close()