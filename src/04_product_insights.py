from pyspark.sql.functions import sum, col
from common.logger import get_logger
from common.spark_session import get_spark
from common.server import keep_server_alive

# Setup
logger = get_logger("phase4_product_insights")
spark = get_spark("Product-Insights")

#  Windows stability configs
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
spark.conf.set("spark.local.dir", "C:/spark-temp")  # ensure this folder exists

logger.info("Phase 4: Product Insights started")

# Read Input Datasets
logger.info("Reading products.csv")
products = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/products.csv")

logger.info("Reading order_items.csv")
order_items = spark.read.option("header", True).option("inferSchema", True) \
    .csv("ecommerce_data/order_items.csv")

# Add Revenue Column
logger.info("Calculating revenue per order item")

order_items_enriched = order_items.withColumn(
    "revenue",
    col("quantity") * col("price")
)

# Join Products with Order Items
logger.info("Joining products with order_items using INNER JOIN")

product_sales = products.join(
    order_items_enriched,
    "product_id",
    "inner"
)

logger.info(f"Product sales row count: {product_sales.count()}")

# 1. Top 10 Selling Products by Revenue
logger.info("Calculating top 10 selling products by revenue")

top_products = product_sales.groupBy(
    "product_id", "product_name"
).agg(
    sum("revenue").alias("total_revenue")
).orderBy(
    col("total_revenue").desc()
)

top_products.show(10, truncate=False)

# 2. Revenue by Product Category
logger.info("Calculating revenue by product category")

category_revenue = product_sales.groupBy(
    "category"
).agg(
    sum("revenue").alias("category_revenue")
).orderBy(
    col("category_revenue").desc()
)

category_revenue.show(truncate=False)

# 3. Products Never Ordered
logger.info("Finding products that were never ordered")

products_never_ordered = products.join(
    order_items,
    "product_id",
    "left_anti"
)

products_never_ordered.show(truncate=False)

# Explain Execution Plan
logger.info("Explaining Spark execution plan for product_sales")
product_sales.explain()

logger.info("Phase 4: Product Insights completed successfully")

#  CLEAN SHUTDOWN (IMPORTANT)
logger.info("Stopping Spark session cleanly")
keep_server_alive(spark, logger)