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

