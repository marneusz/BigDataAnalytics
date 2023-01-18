#! /bin/bash

# ex. args: 2022 2022 10 12 True
python3 ~/repos/BigDataAnalytics/1_data_acquisition/reddit/reddit_historical.py $1 $2 $3 $4 $5
hdfs dfs -mkdir /user/bda_reddit_pw/historical_reddit
hdfs dfs -put -f RS_*  /user/bda_reddit_pw/historical_reddit/
rm RS_*
