--------------------------------------
Table properties:

location :
format :
csv-skip-first-lines :
csv-skip-last-lines :
csv-delimiter :
file-filter-regx :
file-compression-format :
io.trino.metadata.MetadataUtil;
io.trino.sql.tree.QualifiedName
io.trino.spi.connector.SchemaTableName

Xfile Types (csv):
int/long/varchar type
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
    "csv-skip-first-lines" = 1,
    "file-filter-regx" = '.*\.dat',
    format = 'csv'
)
--------------------------------------
select *,
    __file_path__,
    __row_num__
from
    xfile.s2."s3://metastore/example-dat"
where
    __file_path__ like 's3://metastore/exampl%';
--------------------------------------
CREATE TABLE xfile.s2."s3://metastore/example-csv" (
    id varchar,
    name varchar
)
WITH (
    "csv-skip-first-lines" = 1,
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
 CREATE TABLE xfile.s2."s3://metastore/example-parquet" (
    c_customer_sk bigint,
    c_customer_id varchar,
    c_current_cdemo_sk bigint,
    c_current_hdemo_sk bigint,
    c_current_addr_sk bigint,
    c_first_shipto_date_sk bigint,
    c_first_sales_date_sk bigint,
    c_salutation varchar,
    c_first_name varchar,
    c_last_name varchar,
    c_preferred_cust_flag varchar,
    c_birth_day integer,
    c_birth_month integer,
    c_birth_year integer,
    c_birth_country varchar,
    c_login varchar,
    c_email_address varchar,
    c_last_review_date_sk bigint
 )
 WITH (
    "file-filter-regx" = '.*\.parquet$',
    format = 'parquet'
 )
--------------------------------------
 CREATE SCHEMA xfile.s4
 WITH (
    location = 's3://metastore/example-parquet'
    "file-filter-regx" = '.*\.parquet$'
 )
 ------------------------------------