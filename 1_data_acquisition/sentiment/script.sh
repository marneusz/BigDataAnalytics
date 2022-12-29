#!/bin/bash

python3 ~/repos/BigDataAnalytics/1_data_acquisition/sentiment/fetch_sentiment_data.py
hdfs dfs -mkdir /user/bda_reddit_pw/sentiment_data
hdfs dfs -put -f sentiment_data.csv /user/bda_reddit_pw/sentiment_data/
rm sentiment_data.csv
