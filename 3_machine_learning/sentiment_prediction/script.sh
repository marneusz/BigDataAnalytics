#!/bin/bash

# Training sentiment model on sentiment_table and using it on reddit_table
hdfs dfs -mkdir /user/bda_reddit_pw/models/results
hdfs dfs -rm -r /user/bda_reddit_pw/models/sentiment_model
python3 create_sentiment_model.py
