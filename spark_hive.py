from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from os.path import abspath

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

#Create Spark Session to Connect Spark Cluster
spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("Spark_Hive") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .enableHiveSupport() \
        .getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
