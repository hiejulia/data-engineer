hive 
$beeline -u "jdbc:hive2://localhost:10000"


- Run 
beeline -e "hql query"
beeline -f hql_query_file.hql
beeline -i hql_init_file.hql



- Set var
beeline --hivevar
var_name=var_value


!connect <jdbc_url>

!table
show tables; --also support

!column table_name
desc table_name;

!record result_file.dat
!record


dfs -ls;

!run hql_query_file.hql

!quit



- Hive IDE
Connect remove HiveServer 

!connect jdbc:hive2://localhost:10000 scott tiger org.apache.hive.jdbc.HiveDriver

beeline -u 'jdbc:hive2://localhost:10000/default' -n root -p xxx -d org.apache.hive.jdbc.HiveDriver -e "select * from sales;"


- data type

CREATE TABLE test(column1 UNIONTYPE<int, double, array<string>, struct<age:int,country:string>>);




- Create 

CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS]
    [db_name.] table_name
    [(col_name data_type [COMMENT col_comment], ...)]
    [COMMENT table_comment]
    [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
    [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
    [SKEWED BY (col_name, col_name, ...)
    ON ((col_value, col_value, ...), (col_value, col_value, ...), ...)
    [STORED AS DIRECTORIES]
    [
    [ROW FORMAT row_format]
    [STORED AS file_format]
    | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]
    ]
    [LOCATION hdfs_path]
    [TBLPROPERTIES (property_name=property_value, ...)]
    [AS select_statement];




CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database_name.]table_name
  [(column_name data_type [COMMENT column_comment], ...)]
  [PARTITIONED BY (column_name data_type [COMMENT column_comment], ...)];


> CREATE TABLE employee (
      >   name STRING,
      >   work_place ARRAY<STRING>,
      >   gender_age STRUCT<gender:STRING,age:INT>,
      >   skills_score MAP<STRING,INT>,
      >   depart_title MAP<STRING,ARRAY<STRING>>
      > )
      > ROW FORMAT DELIMITED
      > FIELDS TERMINATED BY '|'
      > COLLECTION ITEMS TERMINATED BY ','
      > MAP KEYS TERMINATED BY ':'
      > STORED AS TEXTFILE;


> CREATE DATABASE IF NOT EXISTS myhivebook
      > COMMENT 'hive database demo'
      > LOCATION '/hdfs/directory'
      > WITH DBPROPERTIES ('creator'='dayongd','date'='2018-05-01');


> CREATE TABLE hbase_table_sample(
> id int,
> value1 string,
> value2 string,
> map_value map<string, string>
> )
> STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
> WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val,cf2:val,cf3")
> TBLPROPERTIES ("hbase.table.name" = "table_name_in_hbase");

> ADD JAR mongo-hadoop-core-2.0.2.jar;
> CREATE TABLE mongodb_table_sample(
> id int,
> value1 string,
> value2 string
> )
> STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
> WITH SERDEPROPERTIES (
> 'mongo.columns.mapping'='{"id":"_id","value1":"value1","value2":"value2"}')
> TBLPROPERTIES(
> 'mongo.uri'='mongodb://localhost:27017/default.mongo_sample'
> );


- SerDe


hive>CREATE TABLE web_logs(remote_ip STRING,dt STRING,httpmethod STRING,request STRING,protocol STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES("input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (?:-|\[([^\]]*)\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*)",
"output.format.string" = "%1$s %2$s %3$s %4$s %5$s"
);


JsonSerDe



CREATE EXTERNAL TABLE messages (
msg_id BIGINT,
tstamp STRING,
text STRING,
user_id BIGINT,
user_name STRING
)
ROW FORMAT SERDE "org.apache.hadoop.hive.contrib.serde2.JsonSerde"
WITH SERDEPROPERTIES (
"msg_id"="$.id",
"tstamp"="$.created_at",
"text"="$.text",
"user_id"="$.user.id",
"user_name"="$.user.name"
)
LOCATION '/data/messages';


- CSVSERDE

CREATE TABLE my_table(a string, b string, ...)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\t",
    "quoteChar"     = "'",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE;








!table employee

!column employee


- Load data to table
> LOAD DATA INPATH '/tmp/hivedemo/data/employee.txt' 
      > OVERWRITE INTO TABLE employee;
      

- Query 


> SELECT 
      > name, skills_score['DB'] as DB, skills_score['Perl'] as Perl,
      > skills_score['Python'] as Python, 
      > skills_score['Sales'] as Sales,
      > skills_score['HR'] as HR
      > FROM employee;

> SELECT gender_age.gender, gender_age.age FROM employee;

> SELECT 
      > work_place[0] as col_1, work_place[1] as col_2, 
      > work_place[2] as col_3 
      > FROM employee;

> SELECT
      > name, depart_title['Product'][0] as product_col0, 
      > depart_title['Test'][0] as test_col0 
      > FROM employee;

> SELECT 
      > sum(CASE WHEN gender_age.gender = 'Male'
      > THEN gender_age.age ELSE 0 END)/
      > count(CASE WHEN gender_age.gender = 'Male' THEN 1
      > ELSE NULL END) as male_age_avg 
      > FROM employee;
> SELECT
      > sum(coalesce(gender_age.age,0)) as age_sum,
      > sum(if(gender_age.gender = 'Female',gender_age.age,0)) as 
      female_age_sum
      > FROM employee;




- Data security 
> SELECT 
> name, 
> md5(name) as md5_name, -- 128 bit
> sha1(name) as sha1_name, -- 160 bit
> sha2(name, 256) as sha2_name -- 256 bit
> FROM employee;



> SELECT
 -- big letter to U, small letter to l, number to #
> mask("Card-0123-4567-8910", "U", "l", "#") as m0,
 -- mask first n (4) values where X|x for big/small letter, n for number
> mask_first_n("Card-0123-4567-8910", 4) as m1,
 -- mask last n (4) values
> mask_last_n("Card-0123-4567-8910", 4) as m2,
 -- mask everthing except first n(4) values
> mask_show_first_n("Card-0123-4567-8910", 4) as m3,
 -- mask everthing except last n(4) values
> mask_show_last_n("Card-0123-4567-8910", 4) as m4,
 -- return a hash value - sha 256 hex
> mask_hash('Card-0123-4567-8910') as m5
> ;


> SELECT
> name,
> aes_encrypt(name,'1234567890123456') as encrypted,
> aes_decrypt(
> aes_encrypt(name,'1234567890123456'),
> '1234567890123456') as decrypted
> FROM employee;




- Index 

CREATE INDEX index_ip ON TABLE sales(ip) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;

- 


- Compress
> SET hive.exec.compress.intermediate=true


- Config job 
> SET hive.exec.mode.local.auto=true; -- default false
> SET hive.exec.mode.local.auto.inputbytes.max=50000000;
> SET hive.exec.mode.local.auto.input.files.max=5; -- default 4


> SET hive.exec.parallel=true; -- default false
> SET hive.exec.parallel.thread.number=16; -- default 8

> SET hive.auto.convert.join=true; 
> SET hive.optimize.bucketmapjoin=true; -- default false

> SET hive.input.format=
> org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
> SET hive.auto.convert.sortmerge.join=true;
> SET hive.optimize.bucketmapjoin=true;
> SET hive.optimize.bucketmapjoin.sortedmerge=true;
> SET hive.auto.convert.sortmerge.join.noconditionaltask=true;

> SET hive.optimize.skewjoin=true; --If there is data skew in join, set it to true. Default is false.

> SET hive.skewjoin.key=100000; 

SET hive.execution.engine=<engine>; -- <engine> = mr|tez|spark 

> SET hive.vectorized.execution.enabled=true; -- default false

> SET hive.cbo.enable=true; -- default true after v0.14.0
> SET hive.compute.query.using.stats=true; -- default false
> SET hive.stats.fetch.column.stats=true; -- default false
> SET hive.stats.fetch.partition.stats=true; -- default true







- Data aggregation 






- Data manipulation

LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]


INSERT OVERWRITE TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]] select select_statement FROM from_statement;


INSERT INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)] select select_statement FROM from_statement;

FROM from_statement
INSERT OVERWRITE TABLE tablename1 [PARTITION (partcol1=val1, partcol2=val2 ...) [IF NOT EXISTS]] select select_statement1
[INSERT OVERWRITE TABLE tablename2 [PARTITION ... [IF NOT EXISTS]] select select_statement2]
[INSERT INTO TABLE tablename2 [PARTITION ...] select select_statement2] ...;

- Insert to dynamic partition 

FROM tablename
INSERT OVERWRITE TABLE tablename1 PARTITION(root_partition_name='value',child_partition_name)
SELECT select_statment;


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


FROM sales_region slr
INSERT OVERWRITE TABLE sales PARTITION(dop='2015-10-20', city) SELECT slr.id, slr.firstname, slr.lastname, slr.city;


- Write data to query 

Standard syntax:
INSERT OVERWRITE [LOCAL] DIRECTORY directory1 [ROW FORMAT row_format] [STORED AS file_format]SELECT select_statment FROM from_statment.

Hive extension (multiple inserts):
FROM from_statement
INSERT OVERWRITE [LOCAL] DIRECTORY directory1 select_statement1
[INSERT OVERWRITE [LOCAL] DIRECTORY directory2 select_statement2] ...


INSERT OVERWRITE LOCAL DIRECTORY '/sales'
SELECT sle.id, sle.fname, sle.lname, sle.address
FROM sales sle;




> LOAD DATA LOCAL INPATH
      > '/home/dayongd/Downloads/employee_hr.txt'
      > OVERWRITE INTO TABLE employee_hr;


> LOAD DATA LOCAL INPATH
      > '/home/dayongd/Downloads/employee.txt'
      > OVERWRITE INTO TABLE employee_partitioned
      > PARTITION (year=2018, month=12);



> LOAD DATA INPATH
      > '/tmp/hivedemo/data/employee.txt'
      > INTO TABLE employee; -- Without OVERWRITE, it appends data


> LOAD DATA INPATH
> 'hdfs://localhost:9000/tmp/hivedemo/data/employee.txt'
> OVERWRITE INTO TABLE employee;


> FROM ctas_employee
      > INSERT OVERWRITE TABLE employee
      > SELECT *
      > INSERT OVERWRITE TABLE employee_internal
      > SELECT * 
      > INSERT OVERWRITE TABLE employee_partitioned 
      > PARTITION (year=2018, month=9) -- Insert to static partition
      > SELECT *
      > ; 




> INSERT INTO TABLE employee_partitioned
> PARTITION(year, month)
> SELECT name, array('Toronto') as work_place,
> named_struct("gender","Male","age",30) as gender_age,
> map("Python",90) as skills_score,
> map("R&D",array('Developer')) as depart_title, 
> year(start_date) as year, month(start_date) as month
> FROM employee_hr eh
> WHERE eh.employee_id = 102;


> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output1'


> INSERT OVERWRITE LOCAL DIRECTORY '/tmp/output2'
      > ROW FORMAT DELIMITED FIELDS TERMINATED BY ','



-
Append to HDFS files: $hive -e 'select * from employee'|hdfs dfs -appendToFile - /tmp/test1 

Overwrite HDFS files: $hive -e 'select * from employee'|hdfs dfs -put -f - /tmp/test2


- Export 
> EXPORT TABLE employee TO '/tmp/output5';


> IMPORT EXTERNAL TABLE empolyee_imported_external
      > FROM '/tmp/output5'
      > LOCATION '/tmp/output6';

> EXPORT TABLE employee_partitioned partition
      > (year=2018, month=12) TO '/tmp/output7';

> IMPORT TABLE employee_partitioned_imported
      > FROM '/tmp/output7';




- Data sort


> SET mapred.reduce.tasks = 2; -- Sort by with more than 1 reducer


> SELECT name, employee_id FROM employee_hr CLUSTER BY name;

> SELECT name, start_date
      > FROM employee_hr
      > DISTRIBUTE BY start_date SORT BY name;

> SELECT name, employee_id FROM employee_hr DISTRIBUTE BY 
      employee_id; 



- Function 

> SELECT 
> array_contains(work_place, 'Toronto') as is_Toronto,
> sort_array(work_place) as sorted_array
> FROM employee;


> SELECT TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) as currentdate;


> SELECT
> reverse(split(reverse('/home/user/employee.txt'),'/')[0])
> as linux_file_name;


> SELECT 
> INPUT__FILE__NAME,BLOCK__OFFSET__INSIDE__FILE as OFFSIDE
> FROM employee;


- Transaction 

> SET hive.support.concurrency = true;
> SET hive.enforce.bucketing = true;
> SET hive.exec.dynamic.partition.mode = nonstrict;
> SET hive.txn.manager = 
> org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
> SET hive.compactor.initiator.on = true;
> SET hive.compactor.worker.threads = 1;


> CREATE TABLE employee_trans (
> emp_id int,
> name string,
> start_date date,
> quit_date date,
> quit_flag string
> ) 
> CLUSTERED BY (emp_id) INTO 2 BUCKETS STORED as ORC
> TBLPROPERTIES ('transactional'='true'); -- Also need to set this

> MERGE INTO employee_trans as tar USING employee_update as src
> ON tar.emp_id = src.emp_id
> WHEN MATCHED and src.quit_flag <> 'Y' THEN UPDATE SET start_date src.start_date
> WHEN MATCHED and src.quit_flag = 'Y' THEN DELETE
> WHEN NOT MATCHED THEN INSERT VALUES (src.emp_id, src.name, src.start_date, src.quit_date, src.quit_flag);
No rows affected (174.357 seconds)


> SHOW TRANSACTIONS;







- Data correlation 
-- List all columns match java regular expression
> SET hive.support.quoted.identifiers = none; -- Enable this
> SELECT `^work.*` FROM employee; -- All columns start with work





> SELECT 
      > CASE WHEN gender_age.gender = 'Female' THEN 'Ms.'
      > ELSE 'Mr.' END as title,
      > name, 
      > IF(array_contains(work_place, 'New York'), 'US', 'CA') as 
      country
      > FROM employee;

> SELECT 
> name, gender_age.gender as gender
> FROM employee
> WHERE name IN
> (SELECT name FROM employee WHERE gender_age.gender = 'Male');


> SELECT 
> name, gender_age.gender as gender
> FROM employee a
> WHERE EXISTS (
> SELECT * 
> FROM employee b
> WHERE 
> a.gender_age.gender = b.gender_age.gender AND 
b.gender_age.gender = 'Male'
> ); -- This likes join table a and b with column gender

- Join 
> CREATE TABLE IF NOT EXISTS employee_hr (
      > name string,
      > employee_id int,
      > sin_number string,
      > start_date date
      > )
      > ROW FORMAT DELIMITED
      > FIELDS TERMINATED BY '|';


> LOAD DATA INPATH '/tmp/hivedemo/data/employee_hr.txt'
      > OVERWRITE INTO TABLE employee_hr;


> SELECT 
      > emp.name, emph.sin_number
      > FROM employee emp
      > JOIN employee_hr emph ON emp.name = emph.name; -- Equal Join


-- Join with complex expression in join condition
      -- This is also the way to implement conditional join
      -- Below, conditional ignore row with name = 'Will'
> SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> JOIN employee_hr emph ON 
> IF(emp.name = 'Will', '1', emp.name) = 
> CASE WHEN emph.name = 'Will' THEN '0' ELSE emph.name END;



> SELECT 
      > emp.name, emph.sin_number
      > FROM employee emp
      > JOIN employee_hr emph ON emp.name = emph.name
      > WHERE 
      > emp.name = 'Will';


> SELECT 
      > emp.name, empi.employee_id, emph.sin_number
      > FROM employee emp
      > JOIN employee_hr emph ON emp.name = emph.name
      > JOIN employee_id empi ON emp.name = empi.name;


> SELECT /*+ STREAMTABLE(employee_hr) */
> emp.name, empi.employee_id, emph.sin_number
> FROM employee emp
> JOIN employee_hr emph ON emp.name = emph.name
> JOIN employee_id empi ON emph.employee_id = empi.employee_id;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp -- All rows in left table returned
> LEFT JOIN employee_hr emph ON emp.name = emph.name;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp -- All rows in right table returned
> RIGHT JOIN employee_hr emph ON emp.name = emph.name;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp -- Rows from both side returned
> FULL JOIN employee_hr emph ON emp.name = emph.name;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> CROSS JOIN employee_hr emph;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> JOIN employee_hr emph;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> JOIN employee_hr emph on 1=1;


> SELECT 
> emp.name, emph.sin_number
> FROM employee emp
> CROSS JOIN employee_hr emph 
> WHERE emp.name <> emph.name;



- Map join 
> SELECT 
> /*+ MAPJOIN(employee) */ emp.name, emph.sin_number
> FROM employee emp
> CROSS JOIN employee_hr emph 
> WHERE emp.name <> emph.name;


> SET hive.optimize.bucketmapjoin = true;
> SET hive.optimize.bucketmapjoin.sortedmerge = true;
> SET hive.input.format =
> org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat; 

> SELECT a.name FROM employee a
> LEFT SEMI JOIN employee_id b ON a.name = b.name;

- Optimize join 
join_table:
    table_reference JOIN table_factor [join_condition]
  | table_reference {LEFT|RIGHT|FULL} [OUTER] JOIN table_reference    join_condition
  | table_reference LEFT SEMI JOIN table_reference join_condition
  | table_reference CROSS JOIN table_reference [join_condition] 

table_reference:
    table_factor
  | join_table

  table_factor:
    tbl_name [alias]
  | table_subquery alias
  | ( table_references )

join_condition:
    ON equality_expression



- Skew join 

set 
hive.optimize.skewjoin=true;
set hive.skewjoin.key=100000;

- bucket sort merge map join 
Set hive.enforce.sorting = true;

set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

SELECT /*+ MAPJOIN(table2) */ column1, column2, column3…
FROM table1 [alias_name1] JOIN table2 [alias_name2] 
ON table1 [alias_name1].key = table2 [alias_name2].key


SELECT /*+ MAPJOIN(Sales_orc) */ a.*, b.* FROM Sales a JOIN Sales_orc b ON a.id = b.id;

SELECT /*+ MAPJOIN(Sales_orc, Location) */ a.*, b.*, c.* FROM Sales a JOIN Sales_orc b ON a.id = b.id JOIN Location ON a.id = c.id;


- bucket map join 

set hive.optimize.bucketmapjoin = true
set hive.enforce.bucketing = true



SELECT /*+ MAPJOIN(table2) */ column1, column2, column3
FROM table1 [alias_name1] JOIN table2 [alias_name2]
ON table1 [alias_name1].key = table2 [alias_name2].key

- Map side join

set hive.auto.convert.join=true;

SELECT /*+ MAPJOIN(Sales_orc)*/ a.fname, b.lname FROM Sales a JOIN Sales_orc b ON a.id = b.id;
SELECT a.* FROM Sales a JOIN Sales_orc b ON a.id = b.id and a.fname = b.fname;


- Cross join 

SELECT * FROM Sales a CROSS JOIN Sales_orc b JOIN Location c on a.id = c.id;

- left semi join 

join_condition
  | table_reference LEFT SEMI JOIN table_reference join_condition

  SELECT a.* FROM Sales a LEFT SEMI JOIN Sales_orc b ON a.id = b.id WHERE b.id = 1;









- Union 
> SELECT a.name as nm FROM employee a
> UNION ALL -- Use column alias to make the same name for union
> SELECT b.name as nm FROM employee_hr b;


> SELECT a.name as nm FROM employee a
> UNION -- UNION removes duplicated names and slower
> SELECT b.name as nm FROM employee_hr b;


-- Order by applies to the unioned data
-- When you want to order only one data set,
-- Use order in the subquery
> SELECT a.name as nm FROM employee a
> UNION ALL 
> SELECT b.name as nm FROM employee_hr b
> ORDER BY nm;



- Views 

> CREATE VIEW IF NOT EXISTS employee_skills
> AS
> SELECT 
> name, skills_score['DB'] as DB,
> skills_score['Perl'] as Perl, 
> skills_score['Python'] as Python,
> skills_score['Sales'] as Sales, 
> skills_score['HR'] as HR 
> FROM employee;


> SHOW VIEWS;
      > SHOW VIEWS 'employee_*';



> DESC FORMATTED employee_skills;
      > SHOW CREATE TABLE employee_skills; -- this is recommended

    
> ALTER VIEW employee_skills SET TBLPROPERTIES ('comment'='A 
      view');


> ALTER VIEW employee_skills as SELECT * from employee;

> LATERAL VIEW explode(work_place) wp as workplace;

> SELECT name, workplace FROM employee_internal
> LATERAL VIEW explode(split(null, ',')) wp as workplace;

> SELECT name, workplace FROM employee_internal
> LATERAL VIEW OUTER explode(split(null, ',')) wp as workplace;

EXPLAIN SELECT * FROM sales_view WHERE pid = 'PI_02' OR pid = 'PI_03' ;









- Bucket 

Bucket number = hash_function(bucketing_column) mod num_buckets


> set hive.enforce.bucketing = true; -- This is recommended

CREATE [EXTERNAL] TABLE [db_name.]table_name
    [(col_name data_type [COMMENT col_comment], ...)]
    CLUSTERED BY (col_name data_type [COMMENT col_comment], ...)
INTO N BUCKETS;

CREATE TABLE sales_bucketed (id INT, fname STRING, lname STRING, address STRING,city STRING,state STRING, zip STRING, IP STRING, prod_id STRING, date1 STRING) CLUSTERED BY (id) INTO 10 BUCKETS;


--Prepare table employee_id and its dataset to populate bucket table
> CREATE TABLE employee_id (
> name STRING,
> employee_id INT, 
> work_place ARRAY<STRING>,
> gender_age STRUCT<gender:STRING,age:INT>,
> skills_score MAP<STRING,INT>,
> depart_title MAP<STRING,ARRAY<STRING>>
> )
> ROW FORMAT DELIMITED
> FIELDS TERMINATED BY '|'
> COLLECTION ITEMS TERMINATED BY ','
> MAP KEYS TERMINATED BY ':';

> LOAD DATA INPATH 
> '/tmp/hivedemo/data/employee_id.txt' 
> OVERWRITE INTO TABLE employee_id


--Create the buckets table
> CREATE TABLE employee_id_buckets (
> name STRING,
> employee_id INT,  -- Use this table column as bucket column later
> work_place ARRAY<STRING>,
> gender_age STRUCT<gender:string,age:int>,
> skills_score MAP<string,int>,
> depart_title MAP<string,ARRAY<string >>
> )
> CLUSTERED BY (employee_id) INTO 2 BUCKETS -- Support more columns
> ROW FORMAT DELIMITED
> FIELDS TERMINATED BY '|'
> COLLECTION ITEMS TERMINATED BY ','
> MAP KEYS TERMINATED BY ':';

Populate bucket table 
> INSERT OVERWRITE TABLE employee_id_buckets SELECT * FROM employee_id;
-- Verify the buckets in the HDFS from shell
$hdfs dfs -ls /user/hive/warehouse/employee_id_buckets


- Partition

hive> set hive.mapred.mode=strict;

hive> set hive.mapred.mode=nonstrict;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


hive> SHOW PARTITIONS customer

hive> SHOW PARTITIONS customer PARTITION(country = 'US')

ALTER TABLE table_name ADD [IF NOT EXISTS] PARTITION partition_spec
    [LOCATION 'loc1'] partition_spec [LOCATION 'loc2'] ...;


LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcolumn1=value1, partcolumn2=value2 ...)]

INSERT OVERWRITE TABLE tablename1 [PARTITION (partcolumn1=value1, partcolumn2=value2 ...)] select_statement1 FROM from_statement;

INSERT INTO TABLE tablename1 [PARTITION (partcolumn1=value1, partcolumn2=value2 ...)] select_statement1 FROM from_statement;




- Partition external table






 CREATE TABLE employee_partitioned (
> name STRING,
> work_place ARRAY<STRING>,
> gender_age STRUCT<gender:STRING,age:INT>,
> skills_score MAP<STRING,INT>,
> depart_title MAP<STRING,ARRAY<STRING>> 
-- This is regular column
> )
> PARTITIONED BY (year INT, month INT) 
-- Use lower case partition column
> ROW FORMAT DELIMITED
> FIELDS TERMINATED BY '|'
> COLLECTION ITEMS TERMINATED BY ','
> MAP KEYS TERMINATED BY ':';

> DESC employee_partitioned; 


> SHOW PARTITIONS employee_partitioned; -- Check partitions

> ALTER TABLE employee_partitioned ADD -- Add multiple static 
      partitions
      > PARTITION (year=2018, month=11) PARTITION (year=2018, 
      month=12);


> SHOW PARTITIONS employee_partitioned;

-- Drop partition with PURGE at the end will remove completely
      -- Drop partition will NOT remove data for external table
      -- Drop partition will remove data with partition for internal table
> ALTER TABLE employee_partitioned     
> DROP IF EXISTS PARTITION (year=2018, month=11);

> SHOW PARTITIONS employee_partitioned;



> ALTER TABLE employee_partitioned     
      > DROP IF EXISTS PARTITION (year=2017); -- Drop all partitions in 
      2017


> ALTER TABLE employee_partitioned  
      > DROP IF EXISTS PARTITION (month=9); -- Drop all month is 9

      


> ALTER TABLE employee_partitioned -- Rename exisiting partition
      values
> PARTITION (year=2018, month=12) 
> RENAME TO PARTITION (year=2018,month=10);


> SHOW PARTITIONS employee_partitioned;

> --ALTER TABLE employee_partitioned PARTITION (year=2018) 
      > --RENAME TO PARTITION (year=2017);




> LOAD DATA INPATH '/tmp/hivedemo/data/employee.txt'
      > OVERWRITE INTO TABLE employee_partitioned 
      > PARTITION (year=2018, month=12);

> SELECT name, year, month FROM employee_partitioned; -- Verify data


-- For internal table, we use truncate
> TRUNCATE TABLE employee_partitioned PARTITION 
      (year=2018,month=12);



-- For external table, we have to use hdfs command
> dfs -rm -r -f /user/dayongd/employee_partitioned;


> ALTER TABLE employee_partitioned ADD COLUMNS (work string) 
      CASCADE;
    


> ALTER TABLE employee_partitioned PARTITION COLUMN(year string);

> DESC employee_partitioned; -- Verify the changes





> ALTER TABLE employee_partitioned PARTITION (year=2018) 
      > SET FILEFORMAT ORC;
      > ALTER TABLE employee_partitioned PARTITION (year=2018) 
      > SET LOCATION '/tmp/data';
      > ALTER TABLE employee_partitioned PARTITION (year=2018) ENABLE 
      NO_DROP;
      > ALTER TABLE employee_partitioned PARTITION (year=2018) ENABLE 
      OFFLINE;
      > ALTER TABLE employee_partitioned PARTITION (year=2018) DISABLE 
      NO_DROP;
      > ALTER TABLE employee_partitioned PARTITION (year=2018) DISABLE 
      OFFLINE;
      > ALTER TABLE employee_partitioned PARTITION (year=2018) CONCATENATE;


Dynamic partition 

hive> set hive.exec.dynamic.partition = true;
hive> set hive.exec.dynamic.partition.mode = nonstrict;


hive> create table sales_part_state (id int, fname string, zip string, ip string, pid string) partitioned by (state string) row format delimited fields terminated by '\t';
hive> Insert into sales_part_state partition(state) select id,fname,zip,ip,pid,state from sales;


select * from partition_keys










- Table 
    ALTER TABLE employee_internal SET SERDEPROPERTIES 
      ('field.delim' = '$');


> ALTER TABLE c_employee SET FILEFORMAT RCFILE;


> ALTER TABLE c_employee SET LOCATION 
      'hdfs://localhost:9000/tmp/employee'; 


> ALTER TABLE c_employee ADD COLUMNS (work string);

> ALTER TABLE c_employee REPLACE COLUMNS (name string);

> ALTER TABLE c_employee SET FILEFORMAT ORC; -- Convert to ORC


- Table clean 



- Sampling


SELECT * FROM Sales_orc TABLESAMPLE(BUCKET 1 OUT OF 10 ON id);
SELECT * FROM Sales_orc TABLESAMPLE(BUCKET 3 OUT OF 5 ON id);
SELECT * FROM Sales_orc TABLESAMPLE(BUCKET 3 OUT OF 100 ON id);
SELECT * FROM Sales_orc TABLESAMPLE(BUCKET 3 OUT OF 1000 ON id);
SELECT * FROM Sales_orc TABLESAMPLE(BUCKET 1 OUT OF 10 ON fname);

SELECT count(*) FROM Sales_orc TABLESAMPLE(BUCKET 4 OUT OF 10 ON rand());
SELECT count(*) FROM Sales_orc TABLESAMPLE(BUCKET 5 OUT OF 10 ON rand());
SELECT count(*) FROM Sales_orc TABLESAMPLE(BUCKET 4 OUT OF 10 ON rand());
SELECT count(*) FROM Sales_orc TABLESAMPLE(BUCKET 5 OUT OF 10 ON rand());


SELECT * FROM Sales_orc TABLESAMPLE(10 PERCENT);
SELECT * FROM Sales_orc TABLESAMPLE(10%);
SELECT * FROM Sales_orc TABLESAMPLE(10M);
SELECT * FROM Sales_orc TABLESAMPLE(0.1M);
SELECT * FROM Sales_orc TABLESAMPLE(10 ROWS);
SELECT * FROM Sales_orc TABLESAMPLE(100 ROWS);



- Date time

hive> SELECT date_add('2016-01-30',5);


hive> SELECT datediff('2016-01-30', '2016-01-25');





- Statistics
ANALYZE TABLE [db_name.]tablename [PARTITION(partcol1[=val1], partcol2[=val2], ...)] COMPUTE STATISTICS [FOR COLUMNS][NOSCAN];


ANALYZE TABLE sales COMPUTE STATISTICS;

- statictics for partition table 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


ANALYZE TABLE sales_part PARTITION(pid= 'PI_09') COMPUTE STATISTICS;



DESCRIBE FORMATTED sales_part PARTITION(pid='PI_09');


ANALYZE TABLE sales_part PARTITION(pid) COMPUTE STATISTICS;


- Column statistics

hive> ANALYZE TABLE t1 [PARTITION p1] COMPUTE STATISTICS FOR [COLUMNS c1, c2..]

ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS ip, pid;


ANALYZE TABLE sales_part PARTITION(pid='PI_09') COMPUTE STATISTICS FOR COLUMNS fname, ip;


- Top K statistic

hive> set hive.stats.topk.collect=true;
hive> set hive.stats.topk.num=4;
hive> set hive.stats.topk.minpercent=0;
hive> set hive.stats.topk.poolsize=100;

- Analytics function 


hive> select fname,ip,RANK() OVER (ORDER BY ip) as ranknum, RANK() OVER (PARTITION BY ip order by fname ) from sales ;

select fname,ip,DENSE_RANK() OVER (ORDER BY ip) as densenum, DENSE_RANK() OVER (PARTITION BY ip order by fname) from sales ;

select fname,ip,ROW_NUMBER() OVER (ORDER BY ip), RANK() OVER (ORDER BY ip), DENSE_RANK() OVER (ORDER BY ip) from sales;

SELECT fname, id, NTILE(4) OVER (ORDER BY id DESC) AS quartile FROM sales WHERE ip = '192.168.56.101';


- Window

SELECT fname, ip, COUNT(pid) OVER (PARTITION BY ip ORDER BY fname ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales;

SELECT fname,ip,zip, COUNT(pid) OVER (PARTITION BY ip), COUNT(ip) OVER (PARTITION BY zip) FROM sales;


SELECT fname,pid, LEAD(pid) OVER (PARTITION BY ip ORDER BY ip)
FROM sales;


SELECT fname,pid, LAG(pid) OVER (PARTITION BY ip ORDER BY ip)
FROM sales;


select fname, ip, first_value(pid) over (partition by ip order by fname) as pid from sales;


select fname, ip, last_value(pid) over (partition by ip order by fname) as pid from sales;



- Format 
create table sales ( id int, fname string, lname string, address string, city string, state string, zip string, ip string, pid string, dop string) row format delimited fields terminated by '\t STORED AS TEXTFILE';


create table sales ( id int, fname string, lname string, address string, city string, state string, zip string, ip string, pid string, dop string) row format delimited fields terminated by '\t' STORED AS SEQUENCEFILE;


TEXTFILE
SEQUENCEFILE
RCFILE
ORC
PARQUET
AVRO


