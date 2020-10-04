- RDD


>errorsRDD = inputRDD.filter(lambda x: "error"in x)
>warningsRDD = inputRDD.filter(lambda x: "warning"in x)
>badLinesRDD = errorsRDD.union(warningsRDD)

>sumCount = nums.aggregate((0, 0),(lambda acc, value: (acc[0] + value, acc[1] + 1),(lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))))


- Transformation 
>reduceByKey(func)


>groupByKey()

>combineByKey(createCombiner,mergeValue,mergeCombiners,partitioner)
>mapValues(func)
>flatMapValues(func)
> sortByKey()
> values()
> subtractByKey
> rightOuterJoin
> leftOuterJoin
> cogroup

- Aggregation
> rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] +y[0], x[1] + y[1]))


> sumCount = nums.combineByKey((lambda x: (x,1)),(lambda x, y: (x[0] + y, x[1] + 1)),(lambda x, y: (x[0] + y[0], x[1] + y[1])))
> sumCount.map(lambda key, xy: (key, xy[0]/xy[1])).collectAsMap()

- Parallel
> sc.parallelize(data).reduceByKey(lambda x, y: x + y)
> sc.parallelize(data).reduceByKey(lambda x, y: x + y, 10)


- Join 

- Sort

> rdd.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: str(x))

- Action 
> countByKey()
> collectAsMap()
> lookup(key)


- Data partition

> val userData= sc.sequenceFile[UserID, UserInfo]("hdfs://...").partitionBy(newHashPartitioner(100))   // Create 100 partitions.persist()



- Load


input = sc.textFile("file:///home/holden/repos/spark/README.md")

- Load json 
> importjson
> data = input.map(lambda x: json.loads(x))


> (data.filter(lambda x: x['lovesPandas']).map(lambda x: json.dumps(x))  .saveAsTextFile(outputFile))

- Load csv

importcsvimportStringIO...def loadRecord(line):"""Parse a CSV line"""    input = StringIO.StringIO(line)    reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])return reader.next()input = sc.textFile(inputFile).map(loadRecord)


- Load SequenceFile 

val data = sc.sequenceFile(inFile,  "org.apache.hadoop.io.Text", "org.apache.hadoop.io.IntWritable")


- Hive 

frompyspark.sqlimport HiveContext

hiveCtx = HiveContext(sc)rows = hiveCtx.sql("SELECT name, age FROM users")firstRow = rows.first()print firstRow.name

- Accumulators

- Broadcast Variables

- SparkSQL 
> frompyspark.sqlimport HiveContexthiveCtx = HiveContext(sc)rows = hiveCtx.sql("SELECT key, value FROM mytable")keys = rows.map(lambda row: row[0])


- window

val ipDStream= accessLogsDStream.map(logEntry=> (logEntry.getIpAddress(), 1))val ipCountDStream= ipDStream.reduceByKeyAndWindow(  {(x, y) => x + y}, // Adding elements in the new batches entering the window  {(x, y) => x - y}, // Removing elements from the oldest batches exiting the windowSeconds(30),       // Window durationSeconds(10))       // Slide duration


def updateRunningSum(values:Seq[Long], state:Option[Long]) = {Some(state.getOrElse(0L) + values.size)}val responseCodeDStream= accessLogsDStream.map(log => (log.getResponseCode(), 1L))val responseCodeCountDStream= responseCodeDStream.updateStateByKey(updateRunningSum_)

- Flume

> val events=FlumeUtils.createStream(ssc, receiverHostname, receiverPort)
> val events=FlumeUtils.createPollingStream(ssc, receiverHostname, receiverPort)


- Checkpoint
> ssc.checkpoint("hdfs://...")


- Fault tolerant

> def createStreamingContext() = {  ...val sc =newSparkContext(conf)// Create a StreamingContext with a 1 second batch sizeval ssc =newStreamingContext(sc, Seconds(1))  ssc.checkpoint(checkpointDir)}...val ssc =StreamingContext.getOrCreate(checkpointDir, createStreamingContext_)


