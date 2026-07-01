import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.upper() for col in df.columns]
    return df


class SemanticMetadata:
    def __init__(self, sf_conn):
        self.sf_conn = sf_conn

    def get_columns(self, source_system: str, target_table: str) -> pd.DataFrame:
        query = """
            SELECT
                SOURCE_SYSTEM,
                TARGET_DATABASE,
                TARGET_SCHEMA,
                TARGET_TABLE,
                TARGET_COLUMN,
                TARGET_DATA_TYPE
            FROM DATA_VALIDATION.METADATA.V_COLUMN_CATALOG_LATEST
            WHERE SOURCE_SYSTEM = %s
              AND TARGET_TABLE = %s
            ORDER BY SOURCE_ORDINAL_POSITION
        """

        return normalize_columns(
            pd.read_sql(query, self.sf_conn, params=[source_system, target_table])
        )

    def get_relationships(self, source_system: str) -> pd.DataFrame:
        query = """
            SELECT
                SOURCE_SYSTEM,
                FROM_TABLE,
                FROM_COLUMN,
                TO_TABLE,
                TO_COLUMN,
                RELATIONSHIP_TYPE,
                CONFIDENCE_LABEL,
                MATCH_RATE
            FROM DATA_VALIDATION.INTELLIGENCE.RELATIONSHIP_GRAPH
            WHERE SOURCE_SYSTEM = %s
              AND CONFIDENCE_LABEL IN ('HIGH', 'MEDIUM')
            ORDER BY MATCH_RATE DESC
        """

        return normalize_columns(
            pd.read_sql(query, self.sf_conn, params=[source_system])
        )

    def get_key_candidates(self, source_system: str) -> pd.DataFrame:
        query = """
            SELECT *
            FROM DATA_VALIDATION.INTELLIGENCE.KEY_CANDIDATES
            WHERE SOURCE_SYSTEM = %s
        """

        return normalize_columns(
            pd.read_sql(query, self.sf_conn, params=[source_system])
        )
    
    def get_cardinality(self, source_system: str) -> pd.DataFrame:
        query = """
            SELECT *
            FROM DATA_VALIDATION.INTELLIGENCE.V_RELATIONSHIP_CARDINALITY_LATEST
            WHERE SOURCE_SYSTEM = %s
        """

        return normalize_columns(
            pd.read_sql(query, self.sf_conn, params=[source_system])
        )