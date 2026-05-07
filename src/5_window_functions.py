from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window

from common.logger import get_logger
from common.spark_session import get_spark
from common.server import keep_server_alive

# Setup
logger = get_logger("phase5_window_functions")
spark = get_spark("Window-Functions")

# ✅ Windows stability configs
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
spark.conf.set("spark.local.dir", "C:/spark-temp")  # ensure folder exists

logger.info("Phase 5: Window Functions started")

# Read Datasets
logger.info("Reading orders.csv")
orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/orders.csv")

logger.info("Reading order_items.csv")
order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Join Orders and Order Items
logger.info("Joining orders and order_items on order_id")

df = orders.join(order_items, "order_id")

logger.info(f"Joined dataset count: {df.count()}")

# Add Revenue Column
logger.info("Calculating revenue column")

df = df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

# WINDOW 1: Rank Orders per Customer
logger.info("Applying window function: order ranking per customer")

order_window = Window.partitionBy("customer_id") \
    .orderBy(col("order_date").desc())

df_ranked = df.withColumn(
    "order_rank",
    row_number().over(order_window)
)

df_ranked.select(
    "customer_id", "order_id", "order_date", "order_rank"
).show(10, truncate=False)

# WINDOW 2: Latest Order per Customer
logger.info("Filtering latest order per customer")

latest_orders = df_ranked.filter(col("order_rank") == 1)

latest_orders.select(
    "customer_id", "order_id", "order_date"
).show(10, truncate=False)

# WINDOW 3: Running Total Spend per Customer
logger.info("Calculating running total spend per customer")

running_total_window = Window.partitionBy("customer_id") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running_total = df.withColumn(
    "running_spend",
    sum("revenue").over(running_total_window)
)

df_running_total.select(
    "customer_id", "order_date", "revenue", "running_spend"
).show(10, truncate=False)

# Explain Execution Plan
logger.info("Explaining Spark execution plan for window functions")
df_running_total.explain()

logger.info("Phase 5: Window Functions completed successfully")

#  CLEAN SHUTDOWN
logger.info("Stopping Spark session cleanly")
keep_server_alive(spark, logger)