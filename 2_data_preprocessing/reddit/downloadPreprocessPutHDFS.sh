#!bin/bash

hdfs dfs -copyToLocal /user/bda_reddit_pw/historical_reddit/$1 .
python3 preprocess_reddit.py $1
hdfs dfs -put $1.csv /user/bda_reddit_pw/historical_reddit_processed/csv_data/
