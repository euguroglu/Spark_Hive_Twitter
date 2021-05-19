from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from os.path import abspath

# Hive save foreachBatch udf function
def save_to_hive_table(current_df, epoc_id):
    print("Inside save_to_hive_table function")
    print("Printing epoc_id: ")
    print(epoc_id)

    spark.sql("INSERT INTO TABLE twitter select team, count, start, end from current_df")
    print("Exit out of save_to_hive_table function")

# Kafka Broker/Cluster Details
KAFKA_TOPIC_NAME_CONS = "twittercounter"
KAFKA_TOPIC2_NAME_CONS = "twittercounter2"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

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

#spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
#Preparing schema for tweets
schema = StructType([
    StructField("timestamp_ms", StringType()),
    StructField("text", StringType()),
    StructField("user", StructType([
        StructField("id", LongType()),
        StructField("followers_count", IntegerType()),
        StructField("friends_count", IntegerType()),
        StructField("statuses_count", IntegerType())]))
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

explode_df = value_df.selectExpr("value.timestamp_ms",
                                 "value.text",
                                 "value.user.id",
                                 "value.user.followers_count",
                                 "value.user.friends_count",
                                 "value.user.statuses_count")

def getTeamTag(text):
    if "Fenerbahce" in text or "Fenerbahçe" in text:
        result = "Fenerbahce"
    elif "Galatasaray" in text:
        result = "Galatasaray"
    elif "Besiktas" in text or "Beşiktaş" in text:
        result = "Besiktas"
    else:
        result = "Trabzonspor"
    return result

udfgetTeamTag = udf(lambda tag: getTeamTag(tag), StringType())

explode_df = explode_df.withColumn("timestamp_ms", col("timestamp_ms").cast(LongType()))

df = explode_df.select(
    from_unixtime(col("timestamp_ms")/1000,"yyyy-MM-dd HH:mm:ss").alias("timestamp"),
    col("text"),
    col("id"),
    col("followers_count"),
    col("friends_count").alias("followed_count"),
    col("statuses_count").alias("tweet_count"),
    udfgetTeamTag(col("text")).alias("team")
)

df = df.select("*").withColumn("timestamp", to_timestamp(col("timestamp")))

# Create 2 minutes thumbling window
window_count_df = df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(col("team"),
        window(col("timestamp"),"2 minutes")) \
        .agg(count("team").alias("count"))

window_count_df2 = window_count_df.withColumn("start", expr("window.start"))
window_count_df3 = window_count_df2.withColumn("end", expr("window.end")).drop("window")

#window_count_df3.createOrReplaceTempView("mytempTable")

spark.sql("""CREATE TABLE IF NOT EXISTS twitter
            (team string, count integer, start timestamp, end timestamp)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
                                                """);
# Save data to hive
console_query = window_count_df3 \
    .writeStream \
    .trigger(processingTime='2 minutes') \
    .outputMode("update") \
    .foreachBatch(save_to_hive_table) \
    .start()

console_query.awaitTermination()
