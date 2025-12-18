1.  Create schema that allows auto discovery files as tales;

--- csv, parquets files in s3://hive/warehouse/ods.db will be registered as tales;
 CREATE SCHEMA xfile.css1


2.  Create schema that allows auto discovery files as tales;

--- csv, parquets files in s3://hive/warehouse/ods.db will be registered as tales;
 CREATE SCHEMA xfile.css2
 WITH (
    location = 's3://hive/warehouse/ods.db'
 )


 3.
 ----
  CREATE TABLE xfile.css9."s3://metastore/example-data/users2.csv.gz" (
     id varchar,
     name varchar,
     age varchar
  )
