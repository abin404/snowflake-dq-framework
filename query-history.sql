SELECT
    q.query_id,
    q.user_name,
    q.database_name,
    q.schema_name,
    q.warehouse_name,
    q.start_time,
    q.execution_time,
    
    /* Best-effort table extraction from query text */
    REGEXP_SUBSTR(
        q.query_text,
        '(?i)(from|join)\\s+([a-zA-Z0-9_\\.]+)',
        1,
        1,
        'e',
        2
    ) AS table_name,

    q.query_text
FROM TABLE(
    INFORMATION_SCHEMA.QUERY_HISTORY(
        DATEADD('day', -1, CURRENT_TIMESTAMP()),
        CURRENT_TIMESTAMP()
    )
) q
WHERE q.database_name = '<YOUR_DATABASE>'
ORDER BY q.start_time DESC;
