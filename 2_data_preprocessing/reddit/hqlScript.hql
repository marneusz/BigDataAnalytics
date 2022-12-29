-- creating normal table

DROP TABLE IF EXISTS reddit.external_table_reddit_historical;
CREATE EXTERNAL TABLE reddit.external_table_reddit_historical (subreddit STRING, title STRING, selftext STRING, created_utc STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_reddit_processed';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_reddit_processed/csvData/RS_2014-10.csv' INTO TABLE 
reddit.external_table_reddit_historical;
