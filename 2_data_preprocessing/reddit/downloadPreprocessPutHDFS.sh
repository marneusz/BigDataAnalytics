#!bin/bash


hdfs dfs -copyToLocal /user/bda_reddit_pw/historical_reddit/$1 ~/repos/BigDataAnalytics/2_data_preprocessing/reddit/

python3 processFileToCsv.py $1

python3 ~/repos/BigDataAnalytics/1_data_acquisition/utils/csv_to_json.py -p ~/repos/BigDataAnalytics/2_data_preprocessing/reddit/$1.csv

hdfs dfs -put -f ~/repos/BigDataAnalytics/2_data_preprocessing/reddit/$1.json /user/bda_reddit_pw/historical_reddit_processed/jsonData




