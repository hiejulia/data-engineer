import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from datetime import datetime

parser = argparse.ArgumentParser(description='Ingest S3 to Kafka topic')
parser.add_argument('--date', type=str, help='execution date')
args = parser.parse_args()

try:
    date = datetime.strptime(args.date, '%Y-%m-%d').strftime('%Y-%m-%d')
except:
    sys.exit("Error: Date string missing or invalid (should given as %Y-%m-%d)") 

# List of servers

servers = '192.10.10.1:9093, 192.10.10.2:9093, 192.10.10.3:9093'

spark = SparkSession.builder.appName('test-app').getOrCreate()

datepath = datetime.strptime(date, '%Y-%m-%d').strftime('%Y/%m/%d')
# Read parquet from S3
spark.read.parquet(f"s3a://snapshot/parquet/integration.Person_vPerson/{datepath}/").createOrReplaceTempView('PERSON')

df = spark.sql(f"""
    SELECT
        *
    FROM PERSON
""")

print(f"*** df.count = {df.count()}")

df_json = df.select([df.MPER.alias('key'), to_json(struct([df[f] for f in df.columns])).alias('value')])
# Write to kafka topic
df_json.write.format("kafka").option("kafka.bootstrap.servers", servers).option("topic", "vperson").save()

# Submitting to spark:
"""
For example in airflow-prod0 run:
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 s3-to-kafka.py 10
"""
