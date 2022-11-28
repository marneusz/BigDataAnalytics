#!/bin/bash

python3 fetch_crypto_historical.py

hdfs dfs -put -f data/crypto_historical_data.csv /user/bda_reddit_pw/historical_crypto



