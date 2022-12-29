#! /bin/bash

python3 ~/repos/BigDataAnalytics/1_data_acquisition/reddit/reddit_historical.py $1 $2 $3 $4
hdfs dfs -put -f ~/repos/BigDataAnalytics/1_data_acquisition/reddit/RS_*  /user/bda_reddit_pw/historical_reddit
rm ~/repos/BigDataAnalytics/1_data_acquisition/reddit/RS_*
