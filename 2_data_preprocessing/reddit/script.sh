#!/bin/bash

touch temporaryFileList.txt
hdfs dfs -ls /user/bda_reddit_pw/historical_reddit | grep -Eoh RS_.+ > temporaryFileList.txt
hdfs dfs -mkdir -p /user/bda_reddit_pw/historical_reddit_processed/table
hdfs dfs -mkdir -p /user/bda_reddit_pw/historical_reddit_processed/csv_data
hdfs dfs -rm /user/bda_reddit_pw/historical_reddit_processed/table/*

while read line; do sh downloadPreprocessPutHDFS.sh $line; done < temporaryFileList.txt

hive -f hqlScript.hql
rm RS_*
rm temporaryFileList.txt
