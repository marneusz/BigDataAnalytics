#!bin/bash

hdfs dfs -copyToLocal /user/bda_reddit_pw/historical_crypto/historical_crypto_data.csv .
python3 preprocess_crypto.py historical_crypto_data.csv
hdfs dfs -mkdir /user/bda_reddit_pw/historical_crypto_processed
hdfs dfs -put historical_crypto_data_processed.csv /user/bda_reddit_pw/historical_crypto_processed
rm historical_crypto_data.csv
rm historical_crypto_data_processed.csv
