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




- Create 


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

- Bucket 
> set hive.enforce.bucketing = true; -- This is recommended


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
