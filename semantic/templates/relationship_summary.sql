CREATE OR REPLACE VIEW {{ target_database }}.{{ semantic_schema }}."{{ view_name }}" AS
SELECT
    NULLIF(TRIM(TO_VARCHAR(p."{{ from_column }}")), '') AS PARENT_KEY_VALUE,
    COUNT(*) AS JOINED_ROW_COUNT,
    COUNT(DISTINCT NULLIF(TRIM(TO_VARCHAR(c."{{ to_column }}")), '')) AS DISTINCT_CHILD_KEY_COUNT,
    '{{ from_table }}' AS _SEMANTIC_PARENT_TABLE,
    '{{ to_table }}' AS _SEMANTIC_CHILD_TABLE,
    '{{ from_column }}' AS _SEMANTIC_PARENT_COLUMN,
    '{{ to_column }}' AS _SEMANTIC_CHILD_COLUMN,
    '{{ cardinality_type }}' AS _SEMANTIC_CARDINALITY_TYPE
FROM {{ target_database }}.{{ raw_schema }}."{{ from_table }}" p
JOIN {{ target_database }}.{{ raw_schema }}."{{ to_table }}" c
  ON NULLIF(TRIM(TO_VARCHAR(p."{{ from_column }}")), '') =
     NULLIF(TRIM(TO_VARCHAR(c."{{ to_column }}")), '')
WHERE NULLIF(TRIM(TO_VARCHAR(p."{{ from_column }}")), '') IS NOT NULL
  AND NULLIF(TRIM(TO_VARCHAR(c."{{ to_column }}")), '') IS NOT NULL
GROUP BY 1;