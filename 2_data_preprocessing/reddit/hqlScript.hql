-- creating normal table

DROP TABLE IF EXISTS reddit_table;
CREATE EXTERNAL TABLE reddit_table (cryptocurrency STRING, title STRING, selftext STRING, created_utc FLOAT, `year` INT, `month` INT, `day` INT, `hour` INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_reddit_processed/REDDIT_TABLE';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_reddit_processed/csvData/*.csv' INTO TABLE 
reddit_table;
