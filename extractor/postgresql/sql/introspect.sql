SELECT description
FROM pg_shdescription
WHERE objoid = (SELECT oid
FROM pg_database
WHERE datname = '[DATABASE]');