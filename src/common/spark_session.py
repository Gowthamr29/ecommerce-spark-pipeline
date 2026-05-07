from pyspark.sql import SparkSession

def get_spark(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # UI Config
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")

        # Disable Hadoop native IO (Windows fix)
        .config("spark.hadoop.io.nativeio.NativeIO.enable", "false")
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")

        .getOrCreate()
    )
    return spark