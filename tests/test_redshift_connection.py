from connectors.redshift import get_redshift_connection

conn = get_redshift_connection("REDSHIFT")

with conn.cursor() as cur:
    cur.execute("""
        SELECT
            current_database() AS database_name,
            current_user AS user_name,
            current_schema() AS schema_name;
    """)
    print(cur.fetchall())

conn.close()