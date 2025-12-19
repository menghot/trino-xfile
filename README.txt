1.  Create schema that allows auto discovery files as tales; csv, parquets files in s3://hive/warehouse/ods.db will be registered as tales;
--------------------------------------
CREATE SCHEMA xfile.s1


2.  Create schema that allows auto discovery files as tales.  csv, parquets files in s3://hive/warehouse/ods.db will be registered as tales;
--------------------------------------

CREATE SCHEMA xfile.s2
WITH (
    location = 's3://hive/warehouse/ods.db'
)

3. ------------------------------------
CREATE TABLE xfile.s1."s3://metastore/example-data/users2.csv.gz" (
  id varchar,
  name varchar,
  age varchar
)

--------------------------------------
Table properties:
location :
format :
csv-skip-rows :
csv-delimiter :
file-filter-regx :
file-compression-format :

--------------------------------------
CSV file only support int/long/varchar type