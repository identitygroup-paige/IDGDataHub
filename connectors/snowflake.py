import os
from pathlib import Path
from typing import Optional

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _load_private_key_der(private_key_path: str, passphrase: Optional[str]) -> bytes:
    key_path = Path(private_key_path).expanduser()

    if not key_path.exists():
        raise FileNotFoundError(f"Snowflake private key file not found: {key_path}")

    with key_path.open("rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection(database: Optional[str] = None, schema: Optional[str] = None):
    auth_mode = os.getenv("SNOWFLAKE_AUTH_MODE", "password").strip().lower()

    common = {
        "account": _required_env("SNOWFLAKE_ACCOUNT"),
        "user": _required_env("SNOWFLAKE_USER"),
        "role": _required_env("SNOWFLAKE_ROLE"),
        "warehouse": _required_env("SNOWFLAKE_WAREHOUSE"),
        "database": database or _required_env("SNOWFLAKE_DATABASE"),
        "schema": schema or _required_env("SNOWFLAKE_SCHEMA"),
    }

    if auth_mode == "keypair":
        private_key_der = _load_private_key_der(
            private_key_path=_required_env("SNOWFLAKE_PRIVATE_KEY_PATH"),
            passphrase=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
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

    if auth_mode == "password":
        return snowflake.connector.connect(
            **common,
            password=_required_env("SNOWFLAKE_PASSWORD"),
        )

    raise ValueError(
        "Invalid SNOWFLAKE_AUTH_MODE. "
        "Expected one of: password, externalbrowser, keypair."
    )