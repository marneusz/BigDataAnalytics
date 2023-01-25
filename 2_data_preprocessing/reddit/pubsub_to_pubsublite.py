from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath,
)

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

import time
    
def main():
    
    project_number = 1072423212419
    project_id = "crypto-busting-375023"
    location = "europe-central2"
    subscription_id = "bda-reddit-topic-sub"
    topic_id = "bda-reddit-topic"
    timeout = 5.0
    
    messages = []
    
    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        message.ack()
        messages.append(message.data)
    
    lite_topic_id = "bda-reddit-topic-lite"
    lite_subscription_id = "bda-reddit-sub-lite"

    loc = CloudRegion(location)
    topic_path = TopicPath(project_number, loc, lite_topic_id)
    
    while True:
        messages = []
        
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)

        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening for messages on {subscription_path}..\n")

        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.

        with PublisherClient() as publisher_client:
            for msg in messages:
                api_future = publisher_client.publish(topic_path, msg)
                # result() blocks. To resolve API futures asynchronously, use add_done_callback().
                message_id = api_future.result()
                message_metadata = MessageMetadata.decode(message_id)
                print(
                    f"Published a message to {topic_path} with partition {message_metadata.partition.value} and offset {message_metadata.cursor.offset}."
                )
        time.sleep(10)
    
if __name__ == "__main__":
    main()