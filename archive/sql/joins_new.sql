SELECT *
FROM db_join db
WHERE db.parent_fqdn || '::' || db.child_fqdn NOT IN (
  SELECT dh.parent_fqdn || '::' || dh.child_fqdn
  FROM dh_join dh
) AND db.db_relationship_id NOT IN (
  SELECT dh.physical_nm
  FROM dh_relationship dh
);