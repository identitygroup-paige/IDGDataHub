import os
import pyodbc


def get_sqlserver_connection(prefix: str):
    server = os.getenv(f"{prefix}_SQL_SERVER")
    database = os.getenv(f"{prefix}_SQL_DATABASE")
    user = os.getenv(f"{prefix}_SQL_USER")
    password = os.getenv(f"{prefix}_SQL_PASSWORD")

    if not all([server, database, user, password]):
        raise ValueError(f"Missing SQL Server environment variables for prefix: {prefix}")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)