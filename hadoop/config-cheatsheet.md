- pseudo-distributed mode.
<configuration>
 <property>
  <name>dfs.name.dir</name>
  <value>/Users/opt/hadoop/name/</value>
 </property>
 <property>
  <name>dfs.data.dir</name>
  <value>/Users/opt/hadoop/data/</value>
 </property>
 <property>
  <name>dfs.replication</name>
  <value>1</value>
 </property>
 <property>
  <name>dfs.webhdfs.enabled</name>
  <value>true</value>
 </property>
</configuration>

- jps

- tail -f /Users/bin/hadoop-1.0.4/logs/hadoop-namenode-MacBook-Pro-2.local.log


- HDFS health 
http://localhost:50070/dfshealth.jsp

