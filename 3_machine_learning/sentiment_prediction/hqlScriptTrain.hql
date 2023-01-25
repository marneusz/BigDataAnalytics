DROP TABLE IF EXISTS crypto_sentiment_train_table;
CREATE EXTERNAL TABLE crypto_sentiment_train_table(`date` string, sentiment double, price double, cryptocurrency string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/crypto_sentiment/table_train/';

-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/crypto_sentiment/data/train/*.csv' INTO TABLE crypto_sentiment_train_table;
