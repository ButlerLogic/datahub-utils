SELECT DISTINCT
  pd.description as comment,
  col.table_schema || '.' || col.table_name || '.' || col.column_name as foreign_fqdn,
  col.table_schema as foreign_field_schema,
  col.table_name as foreign_field_entity,
  col.column_name as foreign_field_name,
  col.ordinal_position as col_id,
  rel.table_schema || '.' || rel.table_name || '.' || rel.column_name as source_fqdn,
  rel.table_schema as source_field_schema,
  rel.table_name as source_field_entity,
  rel.column_name as source_field_name,
  kcu.ordinal_position as key_number,
  kcu.constraint_name as name,
  --rco.unique_constraint_name as unique_name, --this is the name of the source primary key
  LOWER(rco.match_option) as on_match,
  LOWER(rco.update_rule) as on_update,
  LOWER(rco.delete_rule) as on_delete,
  LOWER(tco.constraint_type) as type
FROM information_schema.columns col
  LEFT JOIN (
      SELECT kcu.constraint_schema,
    kcu.constraint_name,
    kcu.table_schema,
    kcu.table_name,
    kcu.column_name,
    kcu.ordinal_position,
    kcu.position_in_unique_constraint
  FROM information_schema.key_column_usage kcu
    JOIN information_schema.table_constraints tco
    ON kcu.constraint_schema = tco.constraint_schema
      AND kcu.constraint_name = tco.constraint_name
      AND tco.constraint_type = 'FOREIGN KEY'
      ) as kcu
  ON col.table_schema = kcu.table_schema
    AND col.table_name = kcu.table_name
    AND col.column_name = kcu.column_name
  LEFT JOIN information_schema.referential_constraints rco
  ON rco.constraint_name = kcu.constraint_name
    AND rco.constraint_schema = kcu.table_schema
  LEFT JOIN information_schema.key_column_usage rel
  ON rco.unique_constraint_name = rel.constraint_name
    AND rco.unique_constraint_schema = rel.constraint_schema
    AND rel.ordinal_position = kcu.position_in_unique_constraint
  LEFT JOIN information_schema.table_constraints tco
  ON tco.constraint_schema = kcu.constraint_schema
    AND tco.constraint_name = kcu.constraint_name
  LEFT JOIN pg_catalog.pg_class c
  ON c.relname = rco.constraint_name
  LEFT JOIN pg_catalog.pg_stat_all_indexes psai
  ON c.oid = psai.indexrelid
  LEFT JOIN pg_catalog.pg_constraint pc
  ON psai.relid = pc.conrelid
    AND pc.conname = kcu.constraint_name
  LEFT JOIN pg_catalog.pg_description pd
  ON pc.oid = pd.objoid
WHERE [SCHEMA_FILTER]
  AND kcu.constraint_name IS NOT NULL
ORDER BY
  col.table_schema,
  col.table_name,
  col_id;