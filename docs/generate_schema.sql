SELECT 
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    character_maximum_length,
    numeric_precision,
    is_nullable,
    column_default
FROM 
    information_schema.columns
WHERE 
    table_schema NOT IN ('information_schema', 'pg_catalog')
    AND table_schema NOT LIKE 'staging_%'
ORDER BY 
    table_schema, table_name, ordinal_position;