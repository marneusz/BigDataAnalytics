import argparse
import os
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from datetime import datetime

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/sentiment_model')
parser.add_argument('-r', '--result_path', type=str, default='/user/bda_reddit_pw/crypto_sentiment/data')
parser.add_argument('-d', '--day_shift', type=int, default=14)

def main(model_path, result_path, day_shift):
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('Sentiment Prediction') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    sentiment_model = PipelineModel.load(model_path)
    reddit_df = spark.sql(f'SELECT * FROM reddit_table')
    reddit_df = reddit_df.withColumnRenamed('selftext', 'text')
    
    prediction_raw = sentiment_model.transform(reddit_df)
    prediction = (prediction_raw.withColumn("xs", vector_to_array("probability"))).select(['prediction'] + [F.col("xs")[1]] + ['created_utc'] + ['cryptocurrency'])
    prediction = prediction.withColumnRenamed('xs[1]', 'probability')
    prediction = prediction.withColumn('created_utc', F.date_format(prediction.created_utc.cast('timestamp'), "yyyy-MM-dd HH:00:00"))
    
#     aggregation = prediction.groupBy(['created_utc', 'cryptocurrency']).agg(F.avg('probability'))
#     aggregation = aggregation.withColumnRenamed('avg(probability)', 'sentiment')
    aggregation = prediction.groupBy(['created_utc', 'cryptocurrency']).agg(F.avg('prediction'))
    aggregation = aggregation.withColumnRenamed('avg(prediction)', 'sentiment')
    aggregation = aggregation.withColumnRenamed('cryptocurrency', 'cryptocurrency_a')
    aggregation = aggregation.withColumn("sub_utc", F.col("created_utc") + F.expr(f'INTERVAL {day_shift} DAYS'))
    
    crypto_df = spark.sql(f'SELECT * FROM crypto_table')
    crypto_df = crypto_df.withColumn('date', F.date_format(crypto_df.date.cast('timestamp'), "yyyy-MM-dd HH:00:00"))
    
    train_df = aggregation.join(crypto_df, (aggregation.sub_utc ==  crypto_df.date) & (aggregation.cryptocurrency_a ==  crypto_df.cryptocurrency),"right")
    train_df = train_df.select('date', 'sentiment', 'price', 'cryptocurrency')
    tmp = train_df.toPandas()
    tmp = tmp.interpolate(method='linear').dropna()
    train_df = spark.createDataFrame(tmp)
    train_df = train_df.sort(F.col("date"))

    test_df = aggregation.filter(F.col("sub_utc") > F.lit(datetime.now().isoformat()))
    test_df = test_df.select('sub_utc', 'sentiment', 'cryptocurrency_a')
    test_df = test_df.withColumnRenamed('sub_utc', 'date')
    test_df = test_df.withColumnRenamed('cryptocurrency_a', 'cryptocurrency')
    test_df = test_df.sort(F.col("date"))

    train_path = os.path.join(result_path, 'train')
    test_path = os.path.join(result_path, 'test')

    cmd = f'hdfs dfs -rm -r {train_path}'
    os.system(cmd)
    train_df.write.csv(train_path)

    cmd = f'hdfs dfs -rm -r {test_path}'
    os.system(cmd)
    test_df.write.csv(test_path)

if __name__ == '__main__':
    args = parser.parse_args()
    main(args.model_path, args.result_path, args.day_shift)
