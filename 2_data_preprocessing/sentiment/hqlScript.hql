-- creating normal table

DROP TABLE IF EXISTS sentiment_table;
DROP TABLE IF EXISTS stable;
CREATE EXTERNAL TABLE sentiment_table(Text string, Sentiment int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/sentiment_data/SENTIMENT_TABLE';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/sentiment_processed/sentiment_data_processed.csv' INTO TABLE sentiment_table;
