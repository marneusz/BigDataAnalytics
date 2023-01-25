#!/bin/bash

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/sentiment_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/sentiment_prophet/*
python3 create_crypto_price_model.py -m=/user/bda_reddit_pw/models/sentiment_prophet -T='crypto_sentiment_train_table' -s='True'

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/results/sentiment_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/results/sentiment_prophet/*
python3 predict_crypto_price.py -m=/user/bda_reddit_pw/models/sentiment_prophet -r=/user/bda_reddit_pw/models/results/sentiment_prophet -T='crypto_sentiment_test_table' -s='True'

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/price_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/price_prophet/*
python3 create_crypto_price_model.py -m=/user/bda_reddit_pw/models/price_prophet -T='crypto_sentiment_train_table'

hdfs dfs -mkdir -p /user/bda_reddit_pw/models/results/price_prophet
hdfs dfs -rm -r /user/bda_reddit_pw/models/results/price_prophet/*
python3 predict_crypto_price.py -m=/user/bda_reddit_pw/models/price_prophet -r=/user/bda_reddit_pw/models/results/price_prophet -T='crypto_sentiment_test_table'
