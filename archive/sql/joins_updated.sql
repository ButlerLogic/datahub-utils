SELECT db.db_relationship_id
	, db.parent_fqdn
	, db.child_fqdn
	, coalesce(
      CASE
        WHEN dh."position" IS NULL OR dh."position" = 0 THEN NULL
        ELSE dh."position"
      END,
      CASE
        WHEN db."position" IS NULL OR db."position" = 0 THEN NULL
        ELSE db."position"
      END,
      0
	  ) as "position"
	, coalesce(
      CASE
        WHEN db."cardinality" IS NULL OR length(trim(db."cardinality")) = 0 THEN NULL
        ELSE db."cardinality"
      END,
      CASE
        WHEN dh."cardinality" IS NULL OR length(trim(dh."cardinality")) = 0 THEN NULL
        ELSE dh."cardinality"
      END,
      '1,1,0,-1'
	  ) as "cardinality"
	, p.id as relationship_id
FROM db_join db
  INNER JOIN dh_join dh ON db.db_relationship_id = dh.db_relationship_id
    AND dh.parent_fqdn = db.parent_fqdn
    AND dh.child_fqdn = db.child_fqdn
  INNER JOIN dh_relationship p ON p.physical_nm = dh.db_relationship_id
WHERE dh."position" != db."position"
  OR dh."cardinality" != db."cardinality"
;