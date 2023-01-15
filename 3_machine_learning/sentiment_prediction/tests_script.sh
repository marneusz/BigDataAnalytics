#!/bin/bash

# It should create train test split based on sentiment_table,
# train it on training data, and show some evaluation reuslts for testing data
hdfs dfs -rm -r /user/bda_reddit_pw/models/test_sentiment_model
python3 create_sentiment_model.py -m="/user/bda_reddit_pw/models/test_sentiment_model" -ts=True
