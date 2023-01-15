-- creating normal table

DROP TABLE IF EXISTS reddit_table;
CREATE EXTERNAL TABLE reddit_table (selftext STRING, created_utc FLOAT, cryptocurrency STRING, `year` INT, `month` INT, `day` INT, `hour` INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_reddit_processed/table/';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_reddit_processed/csv_data/*.csv' INTO TABLE reddit_table;