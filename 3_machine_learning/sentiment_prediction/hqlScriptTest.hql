DROP TABLE IF EXISTS crypto_sentiment_test_table;
CREATE EXTERNAL TABLE crypto_sentiment_test_table(`date` string, sentiment double, cryptocurrency string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/crypto_sentiment/table_test/';

-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/crypto_sentiment/data/test/*.csv' INTO TABLE crypto_sentiment_test_table;
