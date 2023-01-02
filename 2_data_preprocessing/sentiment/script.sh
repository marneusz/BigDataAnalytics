#!/bin/bash

hdfs dfs -copyToLocal /user/bda_reddit_pw/sentiment_data/sentiment_data.csv .
python3 ~/repos/BigDataAnalytics/2_data_preprocessing/sentiment/preprocess_sentiment.py sentiment_data.csv
hdfs dfs -mkdir -p /user/bda_reddit_pw/sentiment_processed/table

hdfs dfs -put sentiment_data_processed.csv /user/bda_reddit_pw/sentiment_processed/
rm sentiment_data.csv
rm sentiment_data_processed.csv

hdfs dfs -rm /user/bda_reddit_pw/sentiment_processed/table/*
hive -f hqlScript.hql
