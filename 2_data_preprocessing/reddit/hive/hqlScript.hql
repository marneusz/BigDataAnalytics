DROP TABLE IF EXISTS reddit_table;
CREATE EXTERNAL TABLE reddit_table(selftext string, created_utc float, cryptocurrency string, `year` int, `month` int, `day` int, `hour` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_reddit_processed/csvData';

-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_reddit_processed/csvData/*' INTO TABLE reddit_table;
