SELECT dh.*, dhds.schema
FROM dh_dataitem dh
  LEFT JOIN dh_dataset dhds ON dhds.physical_nm = dh.dataset_id
WHERE dh.physical_nm NOT IN (
  SELECT physical_nm
  FROM db_dataitem db
) AND dhds.schema IS NOT NULL
ORDER BY dh.physical_nm ;