from pyspark.sql import functions as F
from common.logger import get_logger
from common.spark_session import get_spark
from common.server import keep_server_alive

# Setup
logger = get_logger("phase1_ingestion")
spark = get_spark("Ecommerce-Phase1-Ingestion")

# Windows stability configs
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
spark.conf.set("spark.local.dir", "C:/spark-temp")  # make sure this folder exists

logger.info("Phase 1: Data ingestion started")

# Read Data
logger.info("Reading customers data")
customers = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/customers.csv")

logger.info("Reading products data")
products = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/products.csv")

logger.info("Reading orders data")
orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/orders.csv")

logger.info("Reading order_items data")
order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Print Schemas
logger.info("Printing schemas")
customers.printSchema()
products.printSchema()
orders.printSchema()
order_items.printSchema()

# Basic Profiling
logger.info("Running describe() on customers")
customers.describe().show(truncate=False)

logger.info("Running describe() on order_items")
order_items.select("quantity", "price").describe().show(truncate=False)

# Record Counts
logger.info(f"Customers count: {customers.count()}")
logger.info(f"Products count: {products.count()}")
logger.info(f"Orders count: {orders.count()}")
logger.info(f"Order Items count: {order_items.count()}")

# Null Checks
def null_check(df, name):
    logger.info(f"Checking null counts for {name}")
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).show()

null_check(customers, "customers")
null_check(products, "products")
null_check(orders, "orders")
null_check(order_items, "order_items")

# Explain Execution Plans
logger.info("Explaining Spark execution plans")
customers.explain()
products.explain()
orders.explain()
order_items.explain()

logger.info("Phase 1: Data ingestion and validation completed")

#  CLEAN SHUTDOWN (IMPORTANT)
logger.info("Stopping Spark session cleanly")
keep_server_alive(spark, logger)
