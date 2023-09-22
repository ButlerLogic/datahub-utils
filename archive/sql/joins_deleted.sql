SELECT *
FROM dh_join dh
WHERE dh.parent_fqdn || '::' || dh.child_fqdn NOT IN (
  SELECT db.parent_fqdn || '::' || db.child_fqdn
  FROM db_join db
) AND dh.db_relationship_id NOT IN (
  SELECT db.physical_nm
  FROM db_relationship db
);