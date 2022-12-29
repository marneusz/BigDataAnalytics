DROP TABLE IF EXISTS crypto.crypto_historical;
CREATE EXTERNAL TABLE crypto.crypto_historical(`date` string, price double, cryptocurrency string, `year` string, `month` string, `day` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_crypto_processed/crypto_historical_data.json' INTO TABLE crypto.crypto_historical;
