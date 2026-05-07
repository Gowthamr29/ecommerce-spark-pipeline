from pyspark.sql.functions import sum, count, avg, col
from common.logger import get_logger
from common.spark_session import get_spark
from common.server import keep_server_alive

# Setup
logger = get_logger("phase2_order_analytics")
spark = get_spark("Order-Analytics")

#  Windows stability configs
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
spark.conf.set("spark.local.dir", "C:/spark-temp")  # ensure folder exists

logger.info("Phase 2: Order Analytics started")

# Read Data
logger.info("Reading orders.csv")
orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/orders.csv")

logger.info("Reading order_items.csv")
order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Transformations
logger.info("Joining orders with order_items")
order_df = orders.join(order_items, "order_id")

logger.info("Calculating item revenue")
revenue_df = order_df.withColumn(
    "item_revenue",
    col("quantity") * col("price")
)

# Aggregations
logger.info("Computing order analytics by status")
analytics = revenue_df.groupBy("status").agg(
    sum("item_revenue").alias("total_revenue"),
    count("order_id").alias("order_count"),
    avg("item_revenue").alias("avg_order_value")
)

# Output & Debug
analytics.show(truncate=False)

logger.info("Execution plan for revenue_df")
revenue_df.explain()

logger.info("Phase 2: Order Analytics completed successfully")


#  CLEAN SHUTDOWN (IMPORTANT)
logger.info("Stopping Spark session cleanly")
keep_server_alive(spark, logger)
