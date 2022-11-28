hdfs dfs -ls /user/bda_reddit_pw/historical_reddit | grep -Eoh RS_.+ > temporaryFileList.txt

while read line; do sh downloadPreprocessPutHDFS.sh $line; done < temporaryFileList.txt 

rm ~/repos/BigDataAnalytics/2_data_preprocessing/reddit/RS_*
