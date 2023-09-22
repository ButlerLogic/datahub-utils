-- Most common values and null values
SELECT
    schemaname as schema
  , tablename as set
  , attname as item
  , (null_frac * 100) as null_fraction
  , (most_common_vals::text::varchar[])[1] as most_common_value
FROM pg_stats
WHERE [SCHEMA_FILTER]
ORDER BY schemaname, tablename, attname
;
