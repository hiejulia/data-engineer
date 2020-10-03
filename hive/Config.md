- HiveServer2 HA

<property>
    <name>hive.zookeeper.quorum</name>
    <value>Zookeeper client's session timeout in milliseconds	</value>
</property>
<property>
    <name>hive.zookeeper.session.timeout</name>
    <value>Comma separated list of zookeeper quorum</value>
</property>
<property>
    <name>hive.server2.support.dynamic.service.discovery</name>
    <value>true</value>
</property>
<property>
    <name>hive.server2.zookeeper.namespace</name>
    <value>hiveserver2</value>
</property>




<property>
    <name>hive.metastore.uris</name>
    <value>thrift://$Hive_Metastore_Server_Host_Machine_FQDN</value>
    <description> A comma separated list of metastore uris on which metastore service is running.</description>
</property>


- Transaction 


<configuration>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://localhost:3306/hivedb</value>
        <description>metadata is stored in a MySQL server</description>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.jdbc.Driver</value>
        <description>MySQL JDBC driver class</description>
    </property>
    <property>
          <name>javax.jdo.option.ConnectionUserName</name>
          <value>root</value>
          <description>user name for connecting to mysql server</description>
    </property>
    <property>
          <name>javax.jdo.option.ConnectionPassword</name>
          <value>root</value>
          <description>password for connecting to mysql server</description>
     </property>
 <property>
          <name>hive.support.concurrency</name>
          <value>true</value>
     </property>
 <property>
          <name>hive.enforce.bucketing</name>
          <value>true</value>
     </property>
 <property>
          <name>hive.exec.dynamic.partition.mode</name>
          <value>nonstrict</value>
     </property>
 <property>
          <name>hive.txn.manager</name>
          <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
     </property>
 <property>
          <name>hive.compactor.initiator.on</name>
          <value>true</value>
     </property>
 <property>
          <name>hive.compactor.worker.threads</name>
          <value>1</value>
     </property>
</configuration>


$HIVE_HOME/bin/schematool -dbType mysql -initSchema




- Link 

http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.2/bk_hadoop-ha/content/ha-hs2-service-discovery.html.
