from connectors.snowflake import get_snowflake_connection

conn = get_snowflake_connection()

cur = conn.cursor()

cur.execute("""
SELECT
    CURRENT_USER(),
    CURRENT_ROLE(),
    CURRENT_WAREHOUSE(),
    CURRENT_DATABASE(),
    CURRENT_SCHEMA()
""")

print(cur.fetchone())

cur.close()
conn.close()