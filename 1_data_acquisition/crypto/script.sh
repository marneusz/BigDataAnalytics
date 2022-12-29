#!/bin/bash

# default arg: 90 (days)
python3 ~/repos/BigDataAnalytics/1_data_acquisition/crypto/fetch_crypto_historical.py $1
hdfs dfs -mkdir /user/bda_reddit_pw/historical_crypto
hdfs dfs -put -f historical_crypto_data.csv /user/bda_reddit_pw/historical_crypto/
rm historical_crypto_data.csv
