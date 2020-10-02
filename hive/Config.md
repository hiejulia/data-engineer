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




- Link 

http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.3.2/bk_hadoop-ha/content/ha-hs2-service-discovery.html.
