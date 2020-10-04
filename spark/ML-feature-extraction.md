- TF-IDF 

> rdd = sc.wholeTextFiles("data").map(lambda (name, text): text.split())
> tf = HashingTF()
> tfVectors = tf.transform(rdd).cache()
> idf = IDF()
> idfModel = idf.fit(tfVectors)
> tfIdfVectors = idfModel.transform(tfVectors)

- HashingTF

> tf = HashingTF(10000)  # Create vectors of size S = 10,000
> tf.transform(words)

> rdd = sc.wholeTextFiles("data").map(lambda (name, text): text.split())
> tfVectors = tf.transform(rdd)



- Scale Vector
> frompyspark.mllib.featureimport StandardScaler
> scaler = StandardScaler(withMean=True, withStd=True)
> model = scaler.fit(dataset)
> result = model.transform(dataset)

- normalization 
> normalizer().transform(rdd)


- Word2Vec
> Word2Vec.fit(rdd))


- Statistics

> Statistics.colStats(rdd)
> Statistics.corr(rdd, method)
> Statistics.corr(rdd1, rdd2, method)
> Statistics.chiSqTest(rdd)
