from pyspark.sql import SparkSession
from pyspark.sql.functions import array, create_map, col, lit, when
from pyspark.sql.types import BinaryType, StringType
import uuid

project_number = 32329966328
location = "europe-north1"
subscription_id = "test-sub"
topic_id = "test"

spark = SparkSession.builder.appName("write-app").getOrCreate()

sdf = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

# Transform the dataframe to match the required data fields and data types:
# https://github.com/googleapis/java-pubsublite-spark#data-schema
sdf = (
    sdf.withColumn("key", lit("example").cast(BinaryType()))
    .withColumn("data", col("value").cast(StringType()).cast(BinaryType()))
    .withColumnRenamed("timestamp", "event_timestamp")
    # Populate the attributes field. For example, an even value will
    # have {"key1", [b"even"]}.
    .withColumn(
        "attributes",
        create_map(
            lit("key1"),
            array(when(col("value") % 2 == 0, b"even").otherwise(b"odd")),
        ),
    )
    .drop("value")
)

sdf.printSchema()

query = (
    sdf.writeStream.format("pubsublite")
    .option(
        "pubsublite.topic",
        f"projects/{project_number}/locations/{location}/topics/{topic_id}",
    )
    # Required. Use a unique checkpoint location for each job.
    .option("checkpointLocation", "/tmp/app" + uuid.uuid4().hex)
    .outputMode("append")
    .trigger(processingTime="1 second")
    .start()
)

# Wait 60 seconds to terminate the query.
query.awaitTermination(60)
query.stop()
