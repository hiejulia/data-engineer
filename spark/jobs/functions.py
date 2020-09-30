from datetime import timedelta, date, datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import sum, avg, col, last_day, lit, countDistinct, regexp_replace
import pyspark.sql.functions as sf

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

spark = SparkSession.builder.appName("CustomerView").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

"""
Common function 
"""

def createBillingView(viewName='billing_per_day'):
    logger.info("creating billing per day view as %s" % viewName)
    input_path = getNamedPath('billing-per-day')
    spark.read.option("header", "true").csv(input_path)\
        .createOrReplaceTempView(viewName)

def createBillingGroupsView(viewName='ncm'):
    logger.info("creating billing groups view as %s" % viewName)
    input_path = getNamedPath('billing-groups-daily')
    spark.read.option("header", "true").csv(input_path)\
        .createOrReplaceTempView(viewName)

def createContractCountsView(viewName='ccd'):
    logger.info("creating contract counts view as %s" % viewName)
    input_path = getNamedPath('contracts-daily')
    spark.read.option("header", "true").csv(input_path)\
        .createOrReplaceTempView(viewName)

def createContractsView(date, viewName='contracts'):
    logger.info("creating contract view as %s" % viewName)
    input_path = getNamedPath('jpd.dbserver.sacc.contract', date)
    spark.read.parquet(input_path) \
        .createOrReplaceTempView(viewName)

def createNetezzaViews(date):
    logger.info('creating netezza views')
    spark.read.option('quote', "\'").csv(getNamedPath('SOR_INVOICE_ITEM', date), header=True).createOrReplaceTempView('SOR_INVOICE_ITEM')
    spark.read.option('quote', "\'").csv(getNamedPath('sor-gl-product'), header=True).createOrReplaceTempView('SOR_GL_PRODUCT')
    spark.read.option('quote', "\'").csv(getNamedPath('sor-product'), header=True).createOrReplaceTempView('SOR_PRODUCT')

def createMperMapperView(date=date(2018,7,2), viewName='mper_view'):
    logger.info("creating mper-mapping as %s" % viewName)
    input_path = getNamedPath('mper-mapping', date)
    spark.read.parquet(input_path) \
    .select(
        regexp_replace('MPER', 'MPER', '').alias("MPER"),
        regexp_replace('JpdCustomerNumber', 'JPD', '').alias("JPD")
    ) \
    .persist().createOrReplaceTempView(viewName)

def createMdmView(date=date(2018,7,2), viewName='mdm_view'):
    logger.info("creating mdm_view for %s as %s" % (date, viewName))
    input_path = getNamedPath('mdm_view', date)
    spark.read.parquet(input_path).createOrReplaceTempView(viewName)

def dateSlash(date):
    return date.strftime("%Y/%m/%d")

def getBillingGroupsForDate(date):
    try:
        df = initBillingGroupsDf()
        for lims in [(-999999,0), (0, 20), (20, 40), (40, 60), (60, 999999)]:
            row = spark.sql("""
            select
                '{date}' as date,
                '{llim}-{ulim}' as billing,
                sum(case when NET_EUR > {llim} and NET_EUR <= {ulim} then 1 else 0 end) customers,
                count(MPER) total
            from billing_per_day
            """.format(date=date.strftime('%Y-%m-%d'), llim=lims[0], ulim=lims[1]))
            df = df.union(row)
        return df
    except Exception as e:
        logger.info("Failed to execute SQL: %s" % e)
        return None

# TODO use absolute numbers
def getBillingMatrixByDate(date):
    df = initMatrixDf()

    for col_i, counts in enumerate([(0,1), (2,3), (4,5), (6, 999999)]):
        for row_i, eurs in enumerate([(-999999,0), (0,20), (20,40), (40,60), (60, 999999)]):
            col_num = col_i + 1
            row_num = row_i + 1
            row = spark.sql("""
            select
                '{datestr}' date,
                {col_num} col,
                {row_num} row,
                sum(case when ccd.count >= {counts[0]} and ccd.count <= {counts[1]} and ncm.net_eur > {eurs[0]} and ncm.net_eur <= {eurs[1]} then 1 else 0 end) / count(*) percent
            from ccd
            join ncm on ccd.mper=ncm.mper
            where ccd.date = '{datestr}'
            group by ccd.date
            """.format(datestr=date.strftime('%Y-%m-%d'), col_num=col_num, row_num=row_num, counts=counts, eurs=eurs))
            df = df.union(row)

    return df

def getBillingPerMperPerDay(date):
    # The following clause mimicks J. Lehtiranta's script
    # and assumes views
    # mper_view, SOR_INVOICE_ITEM, SOR_PRODUCT, SOR_GL_PRODUCT
    # to exist
    try:
        spark.sql("""
        SELECT
            '{date}' date,
            res_table.OWNER_CUSTOMER_NBR JPD,
            res_table.JPD_PRODUCT_NBR,
            sum(res_table.NET_EUR) as NET_EUR
        FROM (
          SELECT
            SOR.NET_EUR,
            SOR.JPD_PRODUCT_NBR,
            SOR.OWNER_CUSTOMER_NBR
          FROM SOR_INVOICE_ITEM SOR
          left outer join SOR_GL_PRODUCT
          on SOR.GL_PRODUCT_ID = SOR_GL_PRODUCT.GL_PRODUCT_ID
          left outer join SOR_PRODUCT P_BILL
          on BILLING_PRODUCT_NBR = P_BILL.PRODUCT_NBR
          and P_BILL.CURRENT_FLAG = 1
          and P_BILL.SOURCE_STM = 'ABP'
          left outer join SOR_PRODUCT P_JPD
          on JPD_PRODUCT_NBR  = P_JPD.PRODUCT_NBR
          and P_JPD.CURRENT_FLAG = 1
          and P_JPD.SOURCE_STM = 'JPD'
          WHERE
          date_id = '{date}'
          and substr(SUBSCRIBER_CONTRACT_NBR,1,2 ) not in ('C-','B-')
          and SOR.SOURCE_STM = 'ABP'
          and SOR.SOURCE_KEY <> 'dummy'
          and SOR.JPD_PRODUCT_NBR is not NULL
        ) as res_table
        group by
          OWNER_CUSTOMER_NBR,
          JPD_PRODUCT_NBR
        order by
          OWNER_CUSTOMER_NBR,
          JPD_PRODUCT_NBR
        """.format(date=date.strftime('%Y-%m-%d'))) \
        .createOrReplaceTempView('billing_per_jpd')

        df = spark.sql("""
            SELECT b.date, m.MPER, SUM(b.NET_EUR) NET_EUR
            FROM billing_per_jpd b
            JOIN mper_view m ON b.JPD = m.JPD
            GROUP BY m.MPER, b.date
        """)

        return df
    except Exception as e:
        logger.info("Failed to execute SQL: %s" % e)
        return None

def getContractCountsByDate(date):
    # assumes views contracts & mper_view exist
    try:
        df = spark.sql("""
        select '{0}' as date, b.MPER, a.count from (
            select customerid, count(productid) as count
            from (
                select productid, customerid, from_unixtime(opened/1000) as opened, from_unixtime(closed/1000) as closed
                from contracts
            ) where '{0}' >= opened and (closed is null or closed = 'null' or '{0}' <= closed)
            group by customerid
        ) a join mper_view b on customerid = b.JPD
        """.format(date.strftime("%Y-%m-%d")))
        return df
    except Exception as e:
        logger.info("Failed to execute SQL: %s" % e)
        return None

def getContractSummaryByDate(date):
    try:
        df = initContractCountsDf()
        for lims in [(0,1), (2,3), (4,5), (6,999999)]:
            row = spark.sql("""
            select
                date,
                '{llim}-{ulim}' contracts,
                sum(case when count>={llim} and count<={ulim} then 1 else 0 end) customers,
                count(*) total
            from (
                select date, mper, SUM(count) count
                from contract_counts
                group by date, mper
            )
            where date='{date}'
            group by date
            """.format(date=date.strftime('%Y-%m-%d'), llim=lims[0], ulim=lims[1]))
            df = df.union(row)
        return df
    except Exception as e:
        logger.info("Failed to execute SQL: %s" % e)
        return None

def getCustomerCounts():
    try:
        df = spark.sql("""
        select
            date,
            count(distinct mper) mper,
            count(distinct jpd) jpd,
            count(distinct ssn) ssn
        from mdm_view
        group by date
        """)
        return df
    except Exception as e:
        logger.info("Failed to execute SQL: %s" % e)
        return None

def genDates(datestrs):
    for str in datestrs:
        yield datetime.strptime(str, "%Y-%m-%d").date()

def getDates(datestrs):
    return list(genDates(datestrs))

def getDateRange(date0, date1):
    num_range = range(int ((date1 - date0).days) + 1)
    return [ date0 + timedelta(n) for n in num_range]

def getMdmCols(dt):
    input_path = getNamedPath('mdm_view', dt)
    df = spark.read.parquet(input_path)
    return df.select(
        lit(dt.strftime("%Y-%m-%d")),
        'Code',
        'PIC',
        'JpdCustomerNumber',
        df['IsMaster'] == 'True'
    )

def getNamedPath(name, date=date.today()):
    return {
        'billing-per-day': "s3a://hadoop/customer-view/billing-per-day/", #"%s/" % dateSlash(date),
        'billing-groups-daily': "s3a://hadoop/customer-view/billing-groups-daily/", # "%s/" % dateSlash(date),
        'billing-groups': "s3a://hadoop/customer-view/billing-groups/",
        'billing-matrix': "s3a://hadoop/customer-view/billing-matrix/",
        'billing-matrix-daily': "s3a://hadoop/customer-view/billing-matrix-daily/", # "%s/" % dateSlash(date),
        'contracts-daily-summary': "s3a://hadoop/customer-view/contracts-daily-summary/", # "%s/" % dateSlash(date),
        'contracts-daily': "s3a://hadoop/customer-view/contracts-daily/", # "%s/" % dateSlash(date),
        'contracts-summary': "s3a://hadoop/customer-view/contracts-summary/",
        'customers-daily': "s3a://hadoop/customer-view/customers-daily/",
        'jpd.dbserver.sacc.contract': "s3a://hadoop/snapshots-parquet/jpd.dbserver.sacc.contract/%s/" % dateSlash(date),
        'mdm_view': "s3a://hadoop/snapshots-parquet/mdm/%s" % dateSlash(date),
        'mper-mapping': "s3a://hadoop/snapshots-parquet/mdm/mper-mapping/%s/" % dateSlash(date),
        'SOR_INVOICE_ITEM': "s3a://hadoop/input/netezza/history/SOR_INVOICE_ITEM/%s-*" % date.strftime("%Y-%m"),
        'sor-gl-product': 's3a://hadoop/netezza/sor_gl_product.csv.gz',
        'sor-product': 's3a://hadoop/netezza/sor_product.csv.gz'
    }.get(name)

def initBillingDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("mper", StringType(), True),
        StructField("net_eur", DecimalType(), True)
    ]
    schema = StructType(fields)

    df = spark.createDataFrame([], schema) # empty dataframe

    return df

def initBillingGroupsDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("billing", DecimalType(), True),
        StructField("customers", IntegerType(), True),
        StructField("total", IntegerType(), True)
    ]
    schema = StructType(fields)

    df = spark.createDataFrame([], schema) # empty dataframe

    return df

def initContractsDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("mper", StringType(), True),
        StructField("count", IntegerType(), True)
    ]
    schema = StructType(fields)

    df = spark.createDataFrame([], schema) # empty dataframe

    return df

def initContractCountsDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("contracts", StringType(), True),
        StructField("customers", IntegerType(), True),
        StructField("total", IntegerType(), True)
    ]
    schema = StructType(fields)
    df = spark.createDataFrame([], schema) # empty dataframe
    return df

def initCustomerDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("mper", IntegerType(), True),
        StructField("jpd", IntegerType(), True),
        StructField("ssn", IntegerType(), True),
    ]
    schema = StructType(fields)

    df = spark.createDataFrame([], schema) # empty dataframe

    return df

def initMatrixDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("col", IntegerType(), True),
        StructField("row", IntegerType(), True),
        StructField("percent", DecimalType(), True),
    ]
    schema = StructType(fields)

    df = spark.createDataFrame([], schema) # empty dataframe

    return df

def initMdmDf():
    fields = [
        StructField("date", DateType(), True),
        StructField("mper", IntegerType(), True),
        StructField("jpd", IntegerType(), True),
        StructField("ssn", IntegerType(), True),
        StructField("isMaster", BooleanType(), True),
    ]
    schema = StructType(fields)

    df = spark.createDataFrame([], schema) # empty dataframe

    return df

def readCsv(path, header=True):
    try:
        logger.info("Reading %s", path)
        return spark.read.csv(path, header=header)
    except:
        logger.info("Failed to read %s", path)
        return None

def writeCsv(df, output_paths, repartition=8, partitionBy=None, mode="append"):
    if isinstance(output_paths, str): output_paths = [output_paths]
    for output_path in output_paths:
        try:
            logger.info("Building %s", output_path)
            if partitionBy:
                df.repartition(repartition).write.partitionBy(partitionBy).csv(output_path, mode=mode, header=True)
            else:
                df.repartition(repartition).write.csv(output_path, mode=mode, header=True)
            logger.info("Wrote %s", output_path)
        except Exception as e:
            logger.warn("Failed to write %s, %s", (output_path, e))
