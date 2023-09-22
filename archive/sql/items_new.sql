SELECT db.*, dbds.schema
FROM db_dataitem db
  LEFT JOIN db_dataset dbds ON dbds.physical_nm = db.dataset_id
WHERE db.physical_nm NOT IN (
  SELECT physical_nm
  FROM dh_dataitem dh
)
ORDER BY db.physical_nm ;