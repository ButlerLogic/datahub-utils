SELECT
	  m.schemaname as schema
  , pg_catalog.obj_description(to_regnamespace(m.schemaname):: oid) as schema_comment
  , m.matviewname as entity
  , concat(m.schemaname, '.', m.matviewname) as entity_fqdn
  --, c.relname AS table_name
  , a.attname AS "name"
  , concat(m.schemaname, '.', m.matviewname, '.', a.attname) as fqdn
  , a.attnum as "position"
  , pg_catalog.format_type(a.atttypid, a.atttypmod) as "type"
  , CASE
  	  WHEN cc.is_nullable = 'YES' THEN true
  	  ELSE false
    END as "nullable"
  , cc.column_default as "default"
  , cc.character_maximum_length max_length
  , cc.numeric_precision
  , cc.numeric_precision_radix
  , cc.numeric_scale
  , CASE
  	  WHEN cc.identity_cycle = 'YES' THEN true
  	  ELSE false
  	END as identity_cycle
  , cc.identity_increment::BIGINT
  , Lower(cc.identity_generation) as identity_generation
  , cc.identity_start::BIGINT
  , CASE
      WHEN cc.is_identity = 'YES' THEN true
      ELSE false
    END as "identity"
  , cc.identity_minimum::BIGINT
  , cc.identity_maximum::BIGINT
  , cc.udt_name as udt_type
  , cc.datetime_precision::BIGINT
  , cc.interval_type
  , cc.interval_precision::BIGINT
  , 'materialized view'::text
  , true as "view"
  , coalesce(keys.indisprimary, false) as primary_key
  , pg_catalog.obj_description(pgc.oid) as entity_comment
  , pgd.description as comment
  , CASE
      WHEN kcu.column_name is not null then true
      ELSE false
    END as "key"
  , kcu.ordinal_position as key_position
  , LOWER(tco.constraint_type) as "key_type"
  , tco.constraint_name as key_name
  , definition
FROM pg_catalog.pg_matviews m
  INNER JOIN pg_catalog.pg_class c ON c.relname = m.matviewname
  INNER JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
  INNER JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
  CROSS JOIN LATERAL (
    SELECT
        regexp_matches(definition, 'FROM (\w+)\.(\w+)(?: AS)? (.+)', 'g') AS matches
  ) AS match_data
  CROSS JOIN LATERAL (
    SELECT
        matches[1] AS src_schema,
        matches[2] AS src_table,
        matches[3] AS src_column
  ) AS src_info
  INNER JOIN information_schema.columns cc ON cc.column_name = a.attname AND cc.table_schema = src_schema AND cc.table_name = src_table
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
    ON keys.relname = cc.table_name
    AND keys.attname = cc.column_name
    AND keys.nspname = cc.table_schema
  LEFT JOIN pg_catalog.pg_class pgc ON pgc.relname = cc.table_name
  LEFT JOIN pg_catalog.pg_statio_all_tables st
    ON st.schemaname = cc.table_schema
    AND st.relname = cc.table_name
  LEFT JOIN pg_catalog.pg_description pgd
    ON(pgd.objoid = st.relid)
    AND pgd.objsubid = a.attnum
  LEFT JOIN information_schema.key_column_usage kcu
    ON kcu.column_name = cc.column_name
    AND kcu.constraint_schema = cc.table_schema
  LEFT JOIN information_schema.table_constraints tco
    ON tco.constraint_name = kcu.constraint_name
    AND tco.constraint_schema = kcu.constraint_schema
    AND tco.constraint_name = kcu.constraint_name
WHERE c.relkind = 'm'
  AND a.attnum >= 1
  AND n.nspname = m.schemaname
  AND [SCHEMA_FILTER]
ORDER BY
  cc.table_schema,
  cc.table_name,
  a.attnum;