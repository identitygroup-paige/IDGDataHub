CREATE OR REPLACE VIEW {{ target_database }}.{{ semantic_schema }}."{{ view_name }}" AS
SELECT
    p.*,
    OBJECT_CONSTRUCT_KEEP_NULL(c.*) AS _SEMANTIC_CHILD_RECORD,
    '{{ from_table }}' AS _SEMANTIC_PARENT_TABLE,
    '{{ to_table }}' AS _SEMANTIC_CHILD_TABLE,
    '{{ from_column }}' AS _SEMANTIC_PARENT_COLUMN,
    '{{ to_column }}' AS _SEMANTIC_CHILD_COLUMN
FROM {{ target_database }}.{{ raw_schema }}."{{ from_table }}" p
LEFT JOIN {{ target_database }}.{{ raw_schema }}."{{ to_table }}" c
  ON NULLIF(TRIM(TO_VARCHAR(p."{{ from_column }}")), '') =
     NULLIF(TRIM(TO_VARCHAR(c."{{ to_column }}")), '')
 AND NULLIF(TRIM(TO_VARCHAR(p."{{ from_column }}")), '') IS NOT NULL
 AND NULLIF(TRIM(TO_VARCHAR(c."{{ to_column }}")), '') IS NOT NULL;