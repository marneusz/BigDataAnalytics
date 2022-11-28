
DROP TABLE IF EXISTS crypto.crypto_avro;
CREATE TABLE crypto.crypto_avro STORED AS AVRO as SELECT * FROM crypto.cryptotable;


