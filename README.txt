--------------------------------------
Table properties:
location :
format :
csv-skip-rows :
csv-delimiter :
file-filter-regx :
file-compression-format :
io.trino.metadata.MetadataUtil;
io.trino.sql.tree.QualifiedName
io.trino.spi.connector.SchemaTableName
CSV file only support int/long/varchar type
--------------------------------------
CREATE SCHEMA xfile.s1
WITH (
    location = 's3://hive/warehouse/ods.db'
)
------------------------------------
CREATE SCHEMA xfile.s1
WITH (
    location = 's3://hive/warehouse/ods.db'
)
drop schema xfile.s1 CASCADE;
--------------------------------------
create schema xfile.s2;
CREATE TABLE xfile.s2."s3://metastore/example-dat" (
    id varchar,
    name varchar
)
WITH (
    "csv-skip-rows" = 1,
    "file-filter-regx" = '.*\.dat',
    format = 'csv'
)
--------------------------------------
select *,__file_path__ from xfile.s2."s3://metastore/example-dat"
where __file_path__ like 's3://metastore/exampl%';
--------------------------------------
CREATE TABLE xfile.s2."s3://metastore/example-csv" (
    id varchar,
    name varchar
)
WITH (
    "csv-skip-rows" = 1,
    "file-filter-regx" = '.*\.(csv|csv\.gz|csv\.zip)$',
    format = 'csv'
)
------------------------------------
CREATE TABLE xfile.s1."s3://metastore/example-data/users2.csv.gz" (
   id varchar,
   name varchar,
   age varchar
)
--------------------------------------
location
format
file-compression-format
file-filter-regx
csv-skip-rows
csv-delimiter;
--------------------------------------
