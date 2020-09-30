"""customer-counts.py
Usage:
  customer-counts.py <datestr>

Options:
  -h --help             Show this screen.
"""

from docopt import docopt
from pyspark.sql import SparkSession
import MySQLdb
import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

spark = SparkSession.builder.appName("CustomerCounts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

db = MySQLdb.connect(
    host="memsql0",
    user="root",
    passwd="",
    db="superset"
)

arguments = docopt(__doc__, version='customer-counts v0.1')
mydate = arguments['<datestr>']
if not mydate:
    raise SystemExit(0)
# Read parquet file from s3 
input_path = "s3a://hadoop/snapshots-parquet/mdm/%s/" % mydate.replace("-", "/")

logger.info("read %s" % input_path)
spark.read.parquet(input_path).createOrReplaceTempView("mdm_table")

# Cache mdm_table 
spark.catalog.cacheTable("mdm_table")

# query here-----
uniq_mpers = spark.sql("""
    SELECT COUNT(DISTINCT Code) id, COUNT(DISTINCT PIC) ssn FROM mdm_table WHERE IsMaster='True'
""").head()

# USE SPARK SQL 
uniq_mper_with_ssn = spark.sql("""
    SELECT COUNT(DISTINCT Code) mper FROM mdm_table WHERE IsMaster='True' AND PIC IS NOT NULL AND PIC > ''
""").head()

logger.info("count uniq customerid having open contracts")
cursor = db.cursor()
cursor.execute("""
    SELECT COUNT(DISTINCT customerid) jpd FROM contract_mini WHERE '{date}' >= opened
    AND (closed IS null OR '{date}' <= closed)
""".format(date=mydate))
uniq_jpd_with_contracts = cursor.fetchone()

logger.info("count uniq customerid having billing")
cursor = db.cursor()
cursor.execute("""
    SELECT COUNT(DISTINCT PAYER_CUSTOMER_NBR) jpd FROM SOR_INVOICE_HEADER WHERE '{date}' BETWEEN BILLING_PERIOD_START_DT AND BILLING_PERIOD_END_DT
""".format(date=mydate))
uniq_jpd_with_billing = cursor.fetchone()

insert_row = dict(
    date=mydate,
    mper=uniq_mpers["mper"],
    ssn=uniq_mpers["ssn"],
    mper_ssn=uniq_mper_with_ssn["mper"],
    jpd_contr=uniq_jpd_with_contracts[0],
    jpd_bill=uniq_jpd_with_billing[0]
)

logger.info("insert '{date}', {mper}, {ssn}, {mper_ssn}".format(**insert_row))
cursor = db.cursor()
# MySQL db execute 
cursor.execute("""
    INSERT INTO customers2 VALUES ('{date}', {mper}, {ssn}, {mper_ssn}, {jpd_contr}, {jpd_bill})
    ON DUPLICATE KEY UPDATE mper={mper}, ssn={ssn}, mper_ssn={mper_ssn}, jpd_contr={jpd_contr}, jpd_bill={jpd_bill}
""".format(**insert_row)
)
db.commit()
db.close()

logger.info("ok done")
