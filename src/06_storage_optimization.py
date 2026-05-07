import os
import shutil
import time
from common.server import keep_server_alive

from pyspark.sql.functions import col
from common.logger import get_logger
from common.spark_session import get_spark

# Setup
logger = get_logger("phase6_storage")
spark = get_spark("Storage-Optimization")

spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.local.dir", "C:/spark-temp")

spark.sparkContext.setLogLevel("ERROR")

logger.info("Phase 6: Storage Optimization started")

# Read Data
orders = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/orders.csv")

order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Transform
df = orders.join(order_items, "order_id") \
    .withColumn("revenue", col("quantity") * col("price"))

df.show(5, truncate=False)

# Explain Plan (IMPORTANT)
logger.info("Execution Plan:")
df.explain()

# PANDAS SAFE WRITE FUNCTION
def write_data(df, path, fmt="parquet"):
    logger.info(f"Writing data to {path} as {fmt}")

    pdf = df.toPandas()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if fmt == "parquet":
        try:
            import pyarrow
            pdf.to_parquet(path, index=False)
            logger.info("Written as Parquet")
        except Exception:
            logger.warning("Parquet failed, writing CSV instead")
            pdf.to_csv(path.replace(".parquet", ".csv"), index=False)

    elif fmt == "csv":
        pdf.to_csv(path, index=False)
        logger.info("Written as CSV")


# Write CSV
csv_path = "C:/ecommerce-spark/output/orders_data.csv"
write_data(df, csv_path, "csv")

# Write Parquet
parquet_path = "C:/ecommerce-spark/output/orders_data.parquet"
write_data(df, parquet_path, "parquet")

# Simulate Partitioning (Concept Demo)
logger.info("Simulating partitioning by status")

pdf = df.toPandas()

partition_base = "C:/ecommerce-spark/output/partitioned_data"

if os.path.exists(partition_base):
    shutil.rmtree(partition_base)

for status in pdf["status"].unique():
    partition_path = f"{partition_base}/status={status}"
    os.makedirs(partition_path, exist_ok=True)

    pdf[pdf["status"] == status].to_csv(
        f"{partition_path}/data.csv", index=False
    )

logger.info("Partitioning simulation completed")

# File Size Comparison
def get_size(path):
    return os.path.getsize(path) / 1024  # KB

logger.info(f"CSV size: {round(get_size(csv_path), 2)} KB")

if os.path.exists(parquet_path):
    logger.info(f"Parquet size: {round(get_size(parquet_path), 2)} KB")

logger.info("Phase 6 completed successfully")

# CLEAN SHUTDOWN

keep_server_alive(spark, logger)