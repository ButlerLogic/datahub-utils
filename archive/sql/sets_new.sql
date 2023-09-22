SELECT db.*
FROM db_dataset db
WHERE db.physical_nm NOT IN (
  SELECT physical_nm
  FROM dh_dataset dh
)
ORDER BY db.physical_nm ;