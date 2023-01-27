from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

import base64


def main():
    project_number = 1072423212419
    location = "europe-central2"
    lite_subscription_id = "bda-coinbase-sub-lite"
    
    key_file = open("/home/bda_crypto_busters/repos/BigDataAnalytics/2_data_preprocessing/crypto/stream/crypto-busting-375023-6722d6967eca.json", "rb")
    key = base64.b64encode(key_file.read())
    key = key.decode("utf-8")
    
    spark = (
        SparkSession
        .builder
        .config("spark.jars", "/home/bda_crypto_busters/repos/BigDataAnalytics/2_data_preprocessing/crypto/stream/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar")
        .appName("Read Pub/Sub Lite Stream")
        .master("yarn")
        .getOrCreate()
    )

    sdf = (
        spark.readStream.format("pubsublite")
        .option(
            "pubsublite.subscription",
            f"projects/{project_number}/locations/{location}/subscriptions/{lite_subscription_id}",
        )
        .option("gcp.credentials.key", key)
        .load()
    )
    
    sdf = sdf.withColumn("data", sdf.data.cast(StringType()))
    
    query = (
        sdf.writeStream.format("console")
        .outputMode("append")
        .trigger(processingTime="1 second")
        .start()
    )
    
if __name__ == "__main__":
    main()