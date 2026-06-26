import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization


def get_snowflake_connection(database=None, schema=None):
    auth_mode = os.getenv("SNOWFLAKE_AUTH_MODE", "password").lower()

    common = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": database or os.getenv("SNOWFLAKE_DATABASE"),
        "schema": schema or os.getenv("SNOWFLAKE_SCHEMA"),
    }

    if auth_mode == "keypair":
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase.encode() if passphrase else None,
            )

        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            **common,
            private_key=private_key_der,
        )

    if auth_mode == "externalbrowser":
        return snowflake.connector.connect(
            **common,
            authenticator="externalbrowser",
        )

    return snowflake.connector.connect(
        **common,
        password=os.getenv("SNOWFLAKE_PASSWORD"),
    )