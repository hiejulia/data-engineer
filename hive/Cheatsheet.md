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

- Create 

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

