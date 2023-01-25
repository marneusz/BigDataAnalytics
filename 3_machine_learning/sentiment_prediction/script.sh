#!/bin/bash

hdfs dfs -mkdir -p /user/bda_reddit_pw/crypto_sentiment/data
hdfs dfs -rm -r /user/bda_reddit_pw/crypto_sentiment/data/*
hdfs dfs -rm -r /user/bda_reddit_pw/models/sentiment_model

python3 create_sentiment_model.py
python3 prepare_data.py -d=37

hdfs dfs -rm /user/bda_reddit_pw/crypto_sentiment/table_train/*
hdfs dfs -rm /user/bda_reddit_pw/crypto_sentiment/table_test/*

hive -f hqlScriptTrain.hql
hive -f hqlScriptTest.hql
