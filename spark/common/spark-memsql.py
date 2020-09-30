"""spark-memsql.py

Usage:
  spark-memsql.py [--format=<format>] [--header=<header>] <input_path> <output>
  spark-memsql.py [--format=<format>] [--header=<header>] <input_path> <output>

Options:
  -h --help             Show this screen.
  --format=<format>    Specify input format (parquet/csv)  [default: parquet]
  --header=<header>    Specify spark's header option       [default: True]
"""

from docopt import docopt
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

def main():
    arguments = docopt(__doc__, version='spark-memsql.py v0.1')
    input_path = arguments["<input_path>"]
    output = arguments["<output>"]
    format = arguments["--format"] # see defaults in docopt header
    header = arguments["--header"] # see defaults in docopt header

    read(input_path, output, format, header)

def read(input_path, output, format, header):
    spark = SparkSession.builder\
        .appName(f"spark-memsql-{output}") \
        .config("spark.memsql.host", "memsql-aggr0") \
        .config("spark.memsql.user", "root") \
        .config("spark.memsql.password", "") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Read {input_path} as {format}, target table: {output}")
    spark.read.format(format).load(input_path, header=header) \
      .write.format("com.memsql.spark.connector").mode("error") \
    	.save(output)
    logger.info("OK done")

main()
