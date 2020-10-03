bin/spark-submit my_script.py

- Scala build & run 
sbt clean package$SPARK_HOME/bin/spark-submit \  --class com.oreilly.learningsparkexamples.mini.scala.WordCount \  ./target/...(as above) \  ./README.md ./wordcounts

- Deploy spark-submit
> bin/spark-submit [options] <app jar | python file> [app options]
> bin/spark-submit --master spark://host:7077 --executor-memory 10g my_script.py

- Submitting a Python application in YARN client mode
> $ export HADOP_CONF_DIR=/opt/hadoop/conf$ ./bin/spark-submit \  --master yarn \  --py-files somelib-1.2.egg,otherlib-4.4.zip,other-file.py \  --deploy-mode client \  --name "Example Program"\  --queue exampleQueue \  --num-executors 40 \  --executor-memory 10g \  my_script.py "options" "to your application" "go here"


