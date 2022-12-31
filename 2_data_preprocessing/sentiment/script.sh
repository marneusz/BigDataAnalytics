#!/bin/bash

hdfs dfs -copyToLocal /user/bda_reddit_pw/sentiment_data/sentiment_data.csv .
python3 preprocess_sentiment.py sentiment_data.csv
hdfs dfs -mkdir /user/bda_reddit_pw/sentiment_processed
hdfs dfs -put sentiment_data_processed.csv /user/bda_reddit_pw/sentiment_processed


hive -f HQL_NEW.hql
#rm sentiment_data.csv
#rm sentiment_data_processed.csv
