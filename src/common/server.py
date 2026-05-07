def keep_server_alive(spark, logger):
    try:
        ui_url = spark.sparkContext.uiWebUrl
    except:
        ui_url = "http://localhost:4040"

    print("\n=================================================")
    print(f"Spark UI is running at: {ui_url}")
    print("Server is ACTIVE")
    print("Press ENTER to stop Spark and exit")
    print("=================================================\n")

    logger.info(f"Spark UI available at {ui_url}")

    try:
        # ✅ Wait for ENTER instead of CTRL+C
        input("Press ENTER to stop Spark...")

    finally:
        try:
            if spark and spark.sparkContext._jsc is not None:
                spark.stop()
                logger.info("Spark stopped cleanly")
        except Exception:
            logger.warning("Spark already stopped (safe to ignore)")