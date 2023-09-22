SELECT dr.*, ds.schema
FROM dh_relationship dr
  LEFT JOIN db_dataset ds ON ds.physical_nm = dr.dataset_id
WHERE dr.physical_nm  NOT IN (
  SELECT dr2.physical_nm
  FROM db_relationship dr2
);