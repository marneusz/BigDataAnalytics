#!/bin/bash

# hdfs dfs -mkdir -p /user/bda_reddit_pw/models/test_full_prophet
# hdfs dfs -rm -r /user/bda_reddit_pw/models/test_full_prophet/*
# python3 create_crypto_price_model.py -m=/user/bda_reddit_pw/models/test_full_prophet -ds='True' -d='2022-12-30'

# hdfs dfs -mkdir -p /user/bda_reddit_pw/models/results/test_full_prophet
# hdfs dfs -rm -r /user/bda_reddit_pw/models/results/test_full_prophet/*
# python3 predict_crypto_price.py -m=/user/bda_reddit_pw/models/test_full_prophet -ds='True' -d='2022-12-30' -r=/user/bda_reddit_pw/models/results/test_full_prophet

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/test_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/test_prophet/*
python3 create_crypto_price_model.py -m=/user/bda_reddit_pw/models/test_prophet -ds='True' -d='2022-12-30' -T='crypto_sentiment_train_table'

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/results/test_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/results/test_prophet/*
python3 predict_crypto_price.py -m=/user/bda_reddit_pw/models/test_prophet -ds='True' -d='2022-12-30' -r=/user/bda_reddit_pw/models/results/test_prophet -T='crypto_sentiment_train_table'

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/test_sentiment_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/test_sentiment_prophet/*
python3 create_crypto_price_model.py -m=/user/bda_reddit_pw/models/test_sentiment_prophet -ds='True' -d='2022-12-30' -T='crypto_sentiment_train_table' -s='True'

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/results/test_sentiment_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/results/test_sentiment_prophet/*
python3 predict_crypto_price.py -m=/user/bda_reddit_pw/models/test_sentiment_prophet -ds='True' -d='2022-12-30' -r=/user/bda_reddit_pw/models/results/test_sentiment_prophet -T='crypto_sentiment_train_table' -s='True'
