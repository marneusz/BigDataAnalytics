DROP TABLE IF EXISTS crypto.cryptotable;
CREATE EXTERNAL TABLE crypto.cryptotable(`date` string, price double, subreddit string, `year` string, `month` string, `day` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto/';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_crypto_processed/crypto_historical_data.csv' INTO TABLE crypto.cryptotable;
