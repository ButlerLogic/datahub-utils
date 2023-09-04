-- Entities include tables and views
SELECT
  c.table_schema as schema,
  pg_catalog.obj_description(to_regnamespace(c.table_schema):: oid) as schema_comment,
  c.table_name as entity,
  concat(c.table_schema, '.', c.table_name) as entity_fqdn,
  c.column_name as name,
  concat(c.table_schema, '.', c.table_name, '.', c.column_name) as fqdn,
  c.ordinal_position as position,
  c.data_type as type,
  CASE
    WHEN c.is_nullable = 'YES' THEN true
    ELSE false
  END
  as nullable,
  c.column_default as default,
  c.character_maximum_length max_length,
  --c.character_octet_length,
  c.numeric_precision,
  c.numeric_precision_radix,
  c.numeric_scale,
  CASE
    WHEN c.identity_cycle = 'YES' THEN true
    ELSE false
  END
  as identity_cycle,
  c.identity_increment::BIGINT,
  Lower(c.identity_generation) as identity_generation,
  c.identity_start::BIGINT,
  CASE
    WHEN c.is_identity = 'YES' THEN true
    ELSE false
  END
  as identity,
  c.identity_minimum::BIGINT,
  c.identity_maximum::BIGINT,
  c.udt_name as udt_type,
  c.datetime_precision::BIGINT,
  c.interval_type,
  c.interval_precision::BIGINT,
  LOWER(t.table_type) as entity_type,
  CASE
    WHEN t.table_type = 'VIEW' THEN true
    ELSE false
  END
  as view,
  coalesce(keys.indisprimary, false) as primary_key,
  pg_catalog.obj_description(pgc.oid) as entity_comment,
  pgd.description as comment,
  CASE
    WHEN kcu.column_name is not null then true
    ELSE false
  END
  as key,
  kcu.ordinal_position as key_position,
  LOWER(tco.constraint_type) as key_type,
  tco.constraint_name as key_name
FROM information_schema.tables t
  LEFT JOIN information_schema.columns c
    ON t.table_catalog = c.table_catalog
    AND t.table_schema = c.table_schema
    AND t.table_name = c.table_name
  LEFT JOIN(
    SELECT
      indisprimary,
      pg_attribute.attname,
      pg_class.relname, pg_namespace.nspname
      FROM pg_index, pg_class, pg_attribute, pg_namespace
      WHERE
        indrelid = pg_class.oid
        AND pg_class.relnamespace = pg_namespace.oid
        AND pg_attribute.attrelid = pg_class.oid
        AND pg_attribute.attnum = any(pg_index.indkey)
  ) keys
    ON keys.relname = c.table_name
    AND keys.attname = c.column_name
    AND keys.nspname = c.table_schema
  LEFT JOIN pg_catalog.pg_class pgc ON pgc.relname = c.table_name
  LEFT JOIN pg_catalog.pg_statio_all_tables st
    ON st.schemaname = c.table_schema
    AND st.relname = c.table_name
  LEFT JOIN pg_catalog.pg_description pgd
    ON(pgd.objoid = st.relid)
    AND pgd.objsubid = c.ordinal_position
  LEFT JOIN information_schema.key_column_usage kcu
    ON kcu.column_name = c.column_name
    AND kcu.constraint_schema = c.table_schema
  LEFT JOIN information_schema.table_constraints tco
    ON tco.constraint_name = kcu.constraint_name
    AND tco.constraint_schema = kcu.constraint_schema
    AND tco.constraint_name = kcu.constraint_name
WHERE [SCHEMA_FILTER]
ORDER BY
  c.table_schema ASC,
  c.table_name ASC,
  c.ordinal_position ASC;