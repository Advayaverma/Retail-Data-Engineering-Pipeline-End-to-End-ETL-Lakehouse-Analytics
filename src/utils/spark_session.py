"""
Spark Session Factory for Retail Data Engineering Pipeline.
Configures local Spark master, Adaptive Query Execution (AQE),
memory allocation, and Delta Lake catalogs.
"""

import os
import sys

# Ensure site-packages takes precedence over any local folder named pyspark
cwd = os.path.abspath(os.getcwd())
sys_paths_without_cwd = [p for p in sys.path if os.path.abspath(p) != cwd]


def configure_java_home():
    """Ensure JAVA_HOME is set for PySpark JVM initialization."""
    if not os.environ.get("JAVA_HOME"):
        default_java_locations = [
            r"C:\Program Files\Java\jdk-26.0.1",
            r"C:\Program Files\Java\jdk-17",
            r"C:\Program Files\Java\jdk-11",
            r"C:\Program Files\Eclipse Adoptium\jdk-17",
            "/usr/lib/jvm/java-17-openjdk-amd64",
        ]
        for loc in default_java_locations:
            if os.path.isdir(loc):
                os.environ["JAVA_HOME"] = loc
                os.environ["PATH"] = os.path.join(loc, "bin") + os.pathsep + os.environ.get("PATH", "")
                break


def get_spark_session(app_name: str = "RetailLakehousePipeline", enable_delta: bool = True):
    """
    Build and return an optimized local SparkSession with Delta Lake support.
    """
    configure_java_home()

    # Import pyspark from site-packages safely
    old_sys_path = list(sys.path)
    try:
        sys.path = sys_paths_without_cwd
        from pyspark.sql import SparkSession
    finally:
        sys.path = old_sys_path

    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.ui.enabled", "false")  # disable web UI in headless mode

    if enable_delta:
        try:
            builder = builder \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        except Exception:
            pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
