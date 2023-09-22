SELECT dbi.physical_nm
  , dbs.physical_nm as dataset_id
  , dbs."schema"
	, dbi.physical_nm
	, dbi.logical_nm
	, dbi.type
	, dbi.description
	, dbi.is_pk
	, dbi.key_nm
	, dbi.nullable
	, dbi.example
	, dbi.default_val
	, dhi.description as dh_description
	, dhi.id as id
	, dhi.id as item_id
	, dhs.id as set_id
  , CASE
      WHEN ltrim(dbi.type, '_') != ltrim(dhi.type, '_') THEN true
      ELSE false
	  END as type_changed
	, CASE
      WHEN ltrim(dbi.type, '_') != ltrim(dhi.type, '_') THEN ltrim(dbi.type, '_')
      ELSE NULL
	  END as type_database
	, CASE
      WHEN ltrim(dbi.type, '_') != ltrim(dhi.type, '_') THEN ltrim(dhi.type, '_')
      ELSE NULL
	  END as type_datahub
	, CASE
      WHEN dbi.is_pk != dhi.is_pk THEN true
      ELSE false
	  END as pk_changed
	, CASE
      WHEN dbi.is_pk != dhi.is_pk THEN dbi.is_pk
      ELSE NULL
	  END as pk_database
	, CASE
      WHEN dbi.is_pk != dhi.is_pk THEN dhi.is_pk
      ELSE NULL
	  END as pk_datahub
	, CASE
      WHEN trim(dbi.key_nm) != trim(dhi.key_nm) THEN true
      ELSE false
	  END as keyname_changed
	, CASE
      WHEN trim(dbi.key_nm) != trim(dhi.key_nm) THEN trim(dbi.key_nm)
      ELSE NULL
	  END as keyname_database
	, CASE
      WHEN trim(dbi.key_nm) != trim(dhi.key_nm) THEN trim(dhi.key_nm)
      ELSE NULL
	  END as keyname_datahub
	, CASE
      WHEN dbi.nullable != dhi.nullable THEN true
      ELSE false
	  END as nullable_changed
	, CASE
      WHEN dbi.nullable != dhi.nullable THEN dbi.nullable
      ELSE NULL
	  END as nullable_database
	, CASE
      WHEN dbi.nullable != dhi.nullable THEN dhi.nullable
      ELSE NULL
	  END as nullable_datahub
	, CASE
      WHEN trim(dbi.example) != trim(dhi.example) THEN true
      ELSE false
	  END as example_changed
	, CASE
      WHEN trim(dbi.example) != trim(dhi.example) THEN trim(dbi.example)
      ELSE NULL
	  END as example_database
	, CASE
      WHEN trim(dbi.example) != trim(dhi.example) THEN trim(dhi.example)
      ELSE NULL
	  END as example_datahub
	, CASE
      WHEN trim(dbi.default_val) != trim(dhi.default_val) THEN true
      ELSE false
	  END as default_changed
	, CASE
      WHEN trim(dbi.default_val) != trim(dhi.default_val) THEN trim(dbi.default_val)
      ELSE NULL
	  END as default_database
	, CASE
      WHEN trim(dbi.default_val) != trim(dhi.default_val) THEN trim(dhi.default_val)
      ELSE NULL
	  END as default_datahub
FROM db_dataitem dbi
  INNER JOIN db_dataset dbs ON dbs.physical_nm = dbi.dataset_id
  INNER JOIN dh_dataitem dhi ON dhi.dataset_id = dbi.dataset_id AND dhi.physical_nm = dbi.physical_nm
  INNER JOIN dh_dataset dhs ON dhs.physical_nm = dhi.dataset_id
WHERE
  ltrim(dbi.type, '_') != ltrim(dhi.type, '_')
  OR coalesce(dbi.is_pk, false) != coalesce(dhi.is_pk, false)
  OR coalesce(dbi.key_nm, '') != coalesce(dhi.key_nm, '')
  OR coalesce(dbi.nullable, true) != coalesce(dhi.nullable, true)
  OR coalesce(dbi.example, '') != coalesce(dhi.example, '')
  OR coalesce(dbi.default_val, '') != coalesce(dhi.default_val, '')


-- SELECT
  --   dbs.physical_nm as dataset_id
  -- , dbi.physical_nm
  -- , dbs."schema"
	-- , dhi.id as item_id
	-- , dhs.id as set_id
	-- , CASE
	-- 	WHEN ltrim(dbi.type, '_') != ltrim(dhi.type, '_') THEN true
	-- 	ELSE false
	--   END as type_changed
	-- , CASE
	-- 	WHEN ltrim(dbi.type, '_') != ltrim(dhi.type, '_') THEN ltrim(dbi.type, '_')
	-- 	ELSE NULL
	--   END as type_database
	-- , CASE
	-- 	WHEN ltrim(dbi.type, '_') != ltrim(dhi.type, '_') THEN ltrim(dhi.type, '_')
	-- 	ELSE NULL
	--   END as type_datahub
	-- , CASE
	-- 	WHEN dbi.is_pk != dhi.is_pk THEN true
	-- 	ELSE false
	--   END as pk_changed
	-- , CASE
	-- 	WHEN dbi.is_pk != dhi.is_pk THEN dbi.is_pk
	-- 	ELSE NULL
	--   END as pk_database
	-- , CASE
	-- 	WHEN dbi.is_pk != dhi.is_pk THEN dhi.is_pk
	-- 	ELSE NULL
	--   END as pk_datahub
	-- , CASE
	-- 	WHEN trim(dbi.key_nm) != trim(dhi.key_nm) THEN true
	-- 	ELSE false
	--   END as keyname_changed
	-- , CASE
	-- 	WHEN trim(dbi.key_nm) != trim(dhi.key_nm) THEN trim(dbi.key_nm)
	-- 	ELSE NULL
	--   END as keyname_database
	-- , CASE
	-- 	WHEN trim(dbi.key_nm) != trim(dhi.key_nm) THEN trim(dhi.key_nm)
	-- 	ELSE NULL
	--   END as keyname_datahub
	-- , CASE
	-- 	WHEN dbi.nullable != dhi.nullable THEN true
	-- 	ELSE false
	--   END as nullable_changed
	-- , CASE
	-- 	WHEN dbi.nullable != dhi.nullable THEN dbi.nullable
	-- 	ELSE NULL
	--   END as nullable_database
	-- , CASE
	-- 	WHEN dbi.nullable != dhi.nullable THEN dhi.nullable
	-- 	ELSE NULL
	--   END as nullable_datahub
	-- , CASE
	-- 	WHEN trim(dbi.example) != trim(dhi.example) THEN true
	-- 	ELSE false
	--   END as example_changed
	-- , CASE
	-- 	WHEN trim(dbi.example) != trim(dhi.example) THEN trim(dbi.example)
	-- 	ELSE NULL
	--   END as example_database
	-- , CASE
	-- 	WHEN trim(dbi.example) != trim(dhi.example) THEN trim(dhi.example)
	-- 	ELSE NULL
	--   END as example_datahub
	-- , CASE
	-- 	WHEN trim(dbi.default_val) != trim(dhi.default_val) THEN true
	-- 	ELSE false
	--   END as default_changed
	-- , CASE
	-- 	WHEN trim(dbi.default_val) != trim(dhi.default_val) THEN trim(dbi.default_val)
	-- 	ELSE NULL
	--   END as default_database
	-- , CASE
	-- 	WHEN trim(dbi.default_val) != trim(dhi.default_val) THEN trim(dhi.default_val)
	-- 	ELSE NULL
	--   END as default_datahub
--   dbi.physical_nm
-- FROM db_dataitem dbi
  -- INNER JOIN db_dataset dbs ON dbs.physical_nm = dbi.dataset_id
  -- INNER JOIN dh_dataitem dhi ON dhi.dataset_id = dbi.dataset_id AND dhi.physical_nm = dbi.physical_nm
  -- INNER JOIN dh_dataset dhs ON dhs.physical_nm = dhi.dataset_id
-- WHERE
--   ltrim(dbi.type, '_') != ltrim(dhi.type, '_')
--   OR coalesce(dbi.is_pk, false) != coalesce(dhi.is_pk, false)
--   OR coalesce(dbi.key_nm, '') != coalesce(dhi.key_nm, '')
--   OR coalesce(dbi.nullable, true) != coalesce(dhi.nullable, true)
--   OR coalesce(dbi.example, '') != coalesce(dhi.example, '')
--   OR coalesce(dbi.default_val, '') != coalesce(dhi.default_val, '')
;