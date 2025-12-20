--------------------------------------
1.  Create schema that allows auto discovery files as tales;
csv, parquets files in s3://hive/warehouse/ods.db will be registered as tales;
--------------------------------------
CREATE SCHEMA xfile.s1

--------------------------------------
2.  Create schema that allows auto discovery files as tales.
csv, parquets files in s3://hive/warehouse/ods.db will be registered as tales;
--------------------------------------

CREATE SCHEMA xfile.s1
WITH (
    location = 's3://hive/warehouse/ods.db'
)
------------------------------------
3.
------------------------------------
CREATE TABLE xfile.s1."s3://metastore/example-data/users2.csv.gz" (
  id varchar,
  name varchar,
  age varchar
)

------------------------------------
------------------------------------
Table properties:
location :
format :
csv-skip-rows :
csv-delimiter :
file-filter-regx :
file-compression-format :

--------------------------------------
--------------------------------------
io.trino.metadata.MetadataUtil;
io.trino.sql.tree.QualifiedName
io.trino.spi.connector.SchemaTableName

--------------------------------------
--------------------------------------
CSV file only support int/long/varchar type
drop schema xfile.s3 CASCADE;

--------------------------------------
--------------------------------------
create schema xfile.s3;
create table xfile.s2."s3://metastore/example-dat" (id varchar, name varchar)
 with ("csv-skip-rows"=1, format='csv', "file-filter-regx"='.*\.dat');

--------------------------------------
-- predicate pushdown example
--------------------------------------
select *,__file_path__ from xfile.s2."s3://metastore/example-dat"
where __file_path__ like 's3://metastore/exampl%';


--------------------------------------
location
format
file-compression-format
file-filter-regx
csv-skip-rows
csv-delimiter;