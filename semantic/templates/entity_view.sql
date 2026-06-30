CREATE OR REPLACE VIEW {{ target_database }}.{{ semantic_schema }}."{{ view_name }}" AS
SELECT
    *,
    '{{ entity_name }}' AS _SEMANTIC_ENTITY_NAME,
    '{{ source_table }}' AS _SEMANTIC_SOURCE_TABLE
FROM {{ target_database }}.{{ raw_schema }}."{{ source_table }}";