from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, LongType, FloatType, DateType
from pyspark.sql.functions import from_json, col, from_unixtime

import base64


def main():
    project_number = 1072423212419
    location = "europe-central2"
    lite_subscription_id = "bda-reddit-sub-lite"
    
    key_file = open("/home/bda_crypto_busters/repos/BigDataAnalytics/2_data_preprocessing/crypto/stream/crypto-busting-375023-6722d6967eca.json", "rb")
    key = base64.b64encode(key_file.read())
    key = key.decode("utf-8")
    
    spark = (
        SparkSession
        .builder
        .config("spark.jars", "/home/bda_crypto_busters/repos/BigDataAnalytics/2_data_preprocessing/crypto/stream/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar")
        .config("spark.dynamicAllocation.enabled", "false")
        .appName("Read Pub/Sub Lite Stream")
        .master("yarn")
        .getOrCreate()
    )
    spark.sql("add jar gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar")

    df = (
        spark.readStream.format("pubsublite")
        .option(
            "pubsublite.subscription",
            f"projects/{project_number}/locations/{location}/subscriptions/{lite_subscription_id}",
        )
        .option("gcp.credentials.key", key)
        .load()
    )
    df = df.withColumn("data", sdf.data.cast(StringType())).select("data")
    
    JSONschema = StructType([ 
        StructField("id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("text", StringType(), True),
        StructField("time", LongType(), False),
        StructField("upvotes", IntegerType(), False),
        StructField("comments", IntegerType(), False),
        StructField("subreddit", StringType(), False),
    ])
    
    sdf = df.withColumn("JSONData", from_json(col("data"), JSONschema)).select("JSONData.*")
    sdf = sdf.withColumn("time", sdf.time.cast(IntegerType()))
    sdf = sdf.withColumn("time", from_unixtime(col("time")))
    
    query = (
        sdf.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="1 second")
        .start()
    )
    
if __name__ == "__main__":
    main()