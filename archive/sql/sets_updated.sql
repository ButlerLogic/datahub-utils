SELECT db.physical_nm, db."schema", dh."id",
  CASE
    WHEN trim(db.definition) != trim(dh.definition) THEN true
    ELSE false
  END AS differing_definition,
  CASE
    WHEN trim(db.definition) != trim(dh.definition) THEN dh.definition
    ELSE NULL
  END AS dh_definition,
  CASE
    WHEN trim(db.definition) != trim(dh.definition) THEN db.definition
    ELSE NULL
  END AS db_definition
FROM db_dataset AS db
  INNER JOIN dh_dataset AS dh ON db.physical_nm = dh.physical_nm
WHERE
  trim(coalesce(db.definition, '')) != trim(coalesce(dh.definition, ''))
  AND length(trim(coalesce(db.definition, ''))) > 0
;