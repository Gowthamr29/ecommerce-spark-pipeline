from pyspark.sql.functions import sum, count, max, col
from common.logger import get_logger
from common.spark_session import get_spark
from common.server import keep_server_alive

# Set
logger = get_logger("phase3_customer_360")
spark = get_spark("Customer-360")

#  Windows stability configs
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
spark.conf.set("spark.local.dir", "C:/spark-temp")  # ensure this folder exists

logger.info("Phase 3: Customer 360 started")

# Read Input Data
logger.info("Reading customers.csv")
customers = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/customers.csv")

logger.info("Reading orders.csv")
orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/orders.csv")

logger.info("Reading order_items.csv")
order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Customer 360 Join Logic
logger.info("Joining customers -> orders -> order_items")

df = customers \
    .join(orders, "customer_id") \
    .join(order_items, "order_id")

logger.info(f"Joined dataset count: {df.count()}")

# Spend Calculation
logger.info("Calculating customer spend")

df = df.withColumn(
    "spend",
    col("quantity") * col("price")
)

df.show(truncate=False)

# Customer 360 Aggregation
logger.info("Aggregating Customer 360 metrics")

customer_360 = df.groupBy("customer_id", "name").agg(
    sum("spend").alias("total_spend"),
    count("order_id").alias("total_orders"),
    max("order_date").alias("last_order_date")
)

customer_360.show(truncate=False)

# Explain Execution Plan
logger.info("Explaining Spark execution plan for Customer 360")
customer_360.explain()

logger.info("Phase 3: Customer 360 completed successfully")

#  CLEAN SHUTDOWN
logger.info("Stopping Spark session cleanly")
keep_server_alive(spark, logger)