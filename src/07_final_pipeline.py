from pyspark.sql.functions import col, sum, count, round
from common.logger import get_logger
from common.spark_session import get_spark
from common.server import keep_server_alive

import os

# Setup
logger = get_logger("phase7_final_pipeline")
spark = get_spark("Ecommerce-Final-Pipeline")

# Windows safety configs
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.local.dir", "C:/spark-temp")

spark.sparkContext.setLogLevel("ERROR")

logger.info("Phase 7: Final Mini Pipeline started")

# Read Data
customers = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/customers.csv")

orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/orders.csv")

order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Basic Validation
orders = orders.filter(
    col("order_id").isNotNull() & col("customer_id").isNotNull()
).dropDuplicates()

order_items = order_items.filter(
    col("order_id").isNotNull() & col("product_id").isNotNull()
).dropDuplicates()

customers = customers.filter(
    col("customer_id").isNotNull()
).dropDuplicates()

# Transformations (JOINS)
logger.info("Joining customers, orders, and order_items")

df = customers \
    .join(orders, "customer_id") \
    .join(order_items, "order_id")

logger.info(f"Joined dataset count: {df.count()}")

# Revenue Calculation
logger.info("Calculating revenue")

df = df.withColumn(
    "revenue",
    col("quantity") * col("price")
)

# Order-level Analytics
logger.info("Computing order-level analytics")

order_analytics = df.groupBy(
    "order_id", "customer_id", "order_date"
).agg(
    sum("revenue").alias("order_revenue")
)

# Round values
order_analytics = order_analytics.withColumn(
    "order_revenue",
    round(col("order_revenue"), 2)
)

order_analytics.show(10, truncate=False)

# Customer-level Analytics
logger.info("Computing customer-level analytics")

customer_analytics = order_analytics.groupBy(
    "customer_id"
).agg(
    sum("order_revenue").alias("total_spend"),
    count("order_id").alias("order_count")
)

customer_analytics = customer_analytics.withColumn(
    "total_spend",
    round(col("total_spend"), 2)
)

customer_analytics.show(10, truncate=False)

# Explain Plan (IMPORTANT)
logger.info("Execution Plan for pipeline")
df.explain()

# FINAL SAFE WRITE (PANDAS FALLBACK)
def write_data(df, path):
    logger.info(f"Writing data to {path}")

    pdf = df.toPandas()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    try:
        import pyarrow
        pdf.to_parquet(path, index=False)
        logger.info("Written as Parquet")
    except Exception:
        logger.warning("Parquet failed, writing CSV instead")
        pdf.to_csv(path.replace(".parquet", ".csv"), index=False)

# Write Outputs
write_data(
    order_analytics,
    "C:/ecommerce-spark/output/final/order_analytics.parquet"
)

write_data(
    customer_analytics,
    "C:/ecommerce-spark/output/final/customer_analytics.parquet"
)

logger.info("Phase 7: Final Mini Pipeline completed successfully")

# CLEAN SHUTDOWN
keep_server_alive(spark, logger)