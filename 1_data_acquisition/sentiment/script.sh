#!/bin/bash

python3 fetch_sentiment_data.py

hdfs dfs -put -f data/sentiment_data.csv /user/bda_reddit_pw/sentiment_data
