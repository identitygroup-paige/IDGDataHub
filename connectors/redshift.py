import os

import redshift_connector
from dotenv import load_dotenv

load_dotenv()


def get_redshift_connection(env_prefix: str = "REDSHIFT"):
    return redshift_connector.connect(
        iam=True,
        database=os.getenv(f"{env_prefix}_DATABASE"),
        db_user=os.getenv(f"{env_prefix}_DB_USER"),
        cluster_identifier=os.getenv(f"{env_prefix}_CLUSTER_IDENTIFIER"),
        region=os.getenv(f"{env_prefix}_REGION"),
    )