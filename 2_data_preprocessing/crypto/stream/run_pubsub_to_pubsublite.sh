#!/bin/bash

DATAPROC_REGION="europe-central2"
CLUSTER_ID="bda-hdfs"
PROJECT_NUMBER="1072423212419"
PUBSUBLITE_LOCATION=

gcloud dataproc jobs submit pyspark pubsub_to_pubsublite.py \
    --region=$DATAPROC_REGION \
    --cluster=$CLUSTER_ID \
    --jars=gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar \
    --driver-log-levels=root=INFO \
    --properties=spark.master=yarn
