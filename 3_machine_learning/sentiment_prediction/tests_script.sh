#!/bin/bash

hdfs dfs -rm -r /user/bda_reddit_pw/models/test_sentiment_model
python3 create_sentiment_model.py -m="/user/bda_reddit_pw/models/test_sentiment_model" -ts=True
