#!bin/bash


hdfs dfs -copyToLocal /user/bda_reddit_pw/historical_crypto/crypto_historical_data.csv .

python3 preprocess.py

hdfs dfs -put crypto_historical_data.csv /user/bda_reddit_pw/historical_crypto_processed


