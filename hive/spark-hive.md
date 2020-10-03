set hive.execution.engine=spark;

hive> set spark.master=<Spark Master URL>
hive> set spark.eventLog.enabled=true;
hive> set spark.eventLog.dir=<Spark event log folder (must exist)>
hive> set spark.executor.memory=512m;
hive> set spark.serializer=org.apache.spark.serializer.KryoSerializer;

