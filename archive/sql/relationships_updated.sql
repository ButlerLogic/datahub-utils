SELECT db.physical_nm
	, db.dataset_id
  , ds.schema
	, coalesce(
      CASE
        WHEN dh."type"  IS NULL OR length(trim(dh."type")) = 0 THEN NULL
        ELSE dh."type"
      END,
      CASE
        WHEN db."type"  IS NULL OR length(trim(db."type")) = 0 THEN NULL
        ELSE db."type"
      END
	  ) as "type"
	, coalesce(
      CASE
        WHEN dh.comment  IS NULL OR length(trim(dh.comment)) = 0 THEN NULL
        ELSE dh.comment
      END,
      CASE
        WHEN db.comment  IS NULL OR length(trim(db.comment)) = 0 THEN NULL
        ELSE db.comment
      END
	  ) as comment
	, coalesce(
      CASE
        WHEN db.on_update IS NULL OR length(trim(db.on_update)) = 0 THEN NULL
        ELSE db.on_update
      END,
      CASE
        WHEN dh.on_update IS NULL OR length(trim(dh.on_update)) = 0 THEN NULL
        ELSE dh.on_update
      END
	  ) as on_update
	, coalesce(
      CASE
        WHEN db.on_delete IS NULL OR length(trim(db.on_delete)) = 0 THEN NULL
        ELSE db.on_delete
      END,
      CASE
        WHEN dh.on_delete IS NULL OR length(trim(dh.on_delete)) = 0 THEN NULL
        ELSE dh.on_delete
      END
	  ) as on_delete
	, coalesce(
      CASE
        WHEN db.on_match IS NULL OR length(trim(db.on_match)) = 0 THEN NULL
        ELSE db.on_match
      END,
      CASE
        WHEN dh.on_match IS NULL OR length(trim(dh.on_match)) = 0 THEN NULL
        ELSE dh.on_match
      END
	  ) as on_match
	, coalesce(
      CASE
        WHEN db.logical_nm IS NULL OR length(trim(db.logical_nm)) = 0 THEN NULL
        ELSE db.logical_nm
      END,
      CASE
        WHEN dh.physical_nm IS NULL OR length(trim(dh.physical_nm)) = 0 THEN NULL
        ELSE dh.physical_nm
      END,
      CASE
        WHEN db.physical_nm IS NULL OR length(trim(db.physical_nm)) = 0 THEN NULL
        ELSE db.physical_nm
      END,
      CASE
        WHEN dh.physical_nm IS NULL OR length(trim(dh.physical_nm)) = 0 THEN NULL
        ELSE dh.physical_nm
      END
	  ) as logical_nm
	, dh.id
FROM db_relationship db
  INNER JOIN dh_relationship dh ON db.physical_nm = dh.physical_nm
  LEFT JOIN db_dataset ds ON ds.physical_nm = dh.dataset_id
WHERE (
	dh."type" != db."type"
  OR dh.on_update != db.on_update
  OR dh.on_delete != db.on_delete
  OR dh.on_match != db.on_match
);