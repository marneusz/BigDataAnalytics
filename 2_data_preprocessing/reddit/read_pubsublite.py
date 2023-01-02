from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# https://cloud.google.com/pubsub/lite/docs/write-messages-apache-spark#console

# TODO(developer):
project_number = 32329966328
location = "europe-north1"
subscription_id = "reddit-subscription"

spark = SparkSession.builder.appName("Read Pub/Sub Stream").master("yarn").getOrCreate()

sdf = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        f"projects/{project_number}/locations/{location}/subscriptions/{subscription_id}",
    )
    .load()
)

sdf = sdf.withColumn("data", sdf.data.cast(StringType()))

query = (
    sdf.writeStream.format("console")
    .outputMode("append")
    .trigger(processingTime="1 second")
    .start()
)

# Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
query.awaitTermination(120)
query.stop()
