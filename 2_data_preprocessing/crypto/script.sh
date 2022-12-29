#!bin/bash

hdfs dfs -copyToLocal /user/bda_reddit_pw/historical_crypto/historical_crypto_data.csv .
python3 preprocess_crypto.py historical_crypto_data.csv
python3 ~/repos/BigDataAnalytics/2_data_preprocessing/utils/csv_to_json.py -p historical_crypto_data_processed.csv
hdfs dfs -mkdir /user/bda_reddit_pw/historical_crypto_processed
hdfs dfs -put historical_crypto_data_processed.json /user/bda_reddit_pw/historical_crypto_processed
rm historical_crypto_data.csv
rm historical_crypto_data_processed.csv
rm historical_crypto_data_processed.json

hive -f ~/repos/BigDataAnalytics/2_data_preprocessing/crypto/hqlScript.hql
