-- creating normal table

DROP TABLE IF EXISTS stable;
CREATE EXTERNAL TABLE stable(Text string, Sentiment int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/sentiment_data/';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/sentiment_data/sentiment_data.csv' INTO TABLE stable;
