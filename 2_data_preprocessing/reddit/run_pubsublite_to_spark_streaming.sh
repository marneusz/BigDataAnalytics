#!/bin/bash

GOOGLE_APPLICATION_CREDENTIALS="/home/bda_crypto_busters/keys/cloud_compute_key.json"

DATAPROC_REGION="europe-central2"
CLUSTER_ID="bda-hdfs"
PROJECT_NUMBER="1072423212419"

gcloud dataproc jobs submit pyspark pubsublite_to_spark_streaming.py \
    --region=$DATAPROC_REGION \
    --cluster=$CLUSTER_ID \
    --jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar \
    --driver-log-levels=root=INFO \
    --properties=spark.master=yarn
