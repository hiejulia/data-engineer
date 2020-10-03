create 'users', 'cf'

put 'users', 'row1', 'cf:name', 'john'
put 'users', 'row2', 'cf:name', 'mike'
put 'users', 'row3', 'cf:name', 'honey'

scan "users"

CREATE EXTERNAL TABLE hbase_table_users(key string, name string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:name")
TBLPROPERTIES("hbase.table.name" = "users", "hbase.mapred.output.outputtable" = "users");

SELECT * FROM hbase_table_users;

