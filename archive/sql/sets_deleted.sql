SELECT dh.*
FROM dh_dataset dh
WHERE dh.physical_nm NOT IN (
  SELECT physical_nm
FROM db_dataset db
)
ORDER BY dh.physical_nm ;