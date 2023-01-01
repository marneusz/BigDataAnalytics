#!/bin/bash

hdfs dfs -copyToLocal /user/bda_reddit_pw/historical_crypto/historical_crypto_data.csv .
python3 ~/repos/BigDataAnalytics/2_data_preprocessing/crypto/preprocess_crypto.py historical_crypto_data.csv
hdfs dfs -mkdir -p /user/bda_reddit_pw/historical_crypto_processed/table

hdfs dfs -put historical_crypto_data_processed.csv /user/bda_reddit_pw/historical_crypto_processed/
rm historical_crypto_data.csv
rm historical_crypto_data_processed.csv

hdfs dfs -rm historical_crypto_data_processed.csv /user/bda_reddit_pw/historical_crypto_processed/table/*

hive -f ~/repos/BigDataAnalytics/2_data_preprocessing/crypto/hqlScript.hql
#hive -f ~/repos/BigDataAnalytics/2_data_preprocessing/crypto/part_and_avro.hql
