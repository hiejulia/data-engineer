# Streaming
    - Flume 

# Batch 
    - Sqoop 
    - CLI 

# Flume HDFS kafka agent 



# Name node
- namenode-server:50070
- curl http://ip:50070/webhdfs/v1/data/access_logs/big_log/access.log

# Data node 




# How to run 
- export env vars 
    - HADOOP_STREAMING_JAR = path to hadoop streaming jar 
    - yarn jar $HADOOP_STREAMING_JAR 
        -mapper 'wc -l'
        -reducer awk line_count+= $1 END print line count
        -file reduce.sh
        -numreduceTasks 0 /1 
        -input /data/wiki/en_articles 
        -output wc_mr
- hdfs 
    - hdfs dfs -ls wc_mr 

- hdfs dfs -text wc_mr/*




# Architecture system 
- reboot 
- byzantine 


# Combiner - Partitioner - Comparator 




