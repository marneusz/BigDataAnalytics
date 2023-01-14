import pandas as pd
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
import datetime
import os

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

# arguments

parser.add_argument('-s', '--sentiment', type=str, default='False')
parser.add_argument('-T', '--table', type=str, default='crypto_table')
parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/prophet_model')
parser.add_argument('-r', '--results_path', type=str, default='/user/bda_reddit_pw/models/results/prophet_results')
parser.add_argument('-d', '--start_date', type=str, default=datetime.now().strftime('%Y-%m-%d'))


@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def test_model(data):
    model_name_curr = f'prophet_model_{data.cryptocurrency[0]}.json'

    cmd = f'hdfs dfs -copyToLocal {model_path}/{model_name_curr} .'
    os.system(cmd)
    with open(model_name_curr, 'r') as fin:
        model = model_from_json(fin.read())
    os.remove(model_name_curr)

    future = model.predict(data)

    f_pd = future[['ds', 'yhat', 'yhat_upper', 'yhat_lower']]
    if use_sentiment:
        st_pd = data[['ds', 'cryptocurrency', 'y', 'sentiment']]
    else:
        st_pd = data[['ds', 'cryptocurrency', 'y']]

    result_pd = f_pd.join(st_pd.set_index('ds'), on='ds', how='left')
    result_pd['cryptocurrency'] = data['cryptocurrency'].iloc[0]
    if use_sentiment:
        return result_pd[['cryptocurrency', 'sentiment', 'ds', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]
    return result_pd[['cryptocurrency', 'ds', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


def main(table, start_date, results_path):
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('Crypto Price Prediction') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    if use_sentiment:
        query_str = f'SELECT date as ds, price as y, cryptocurrency, sentiment FROM {table}'
    else:
        query_str = f'SELECT date as ds, price as y, cryptocurrency FROM {table}'

    test_df = spark.sql(f'{query_str} WHERE date>="{start_date.isoformat()}"')
    test_df = test_df.withColumn('ds', test_df.ds.cast('timestamp'))
    test_df = (test_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    result = test_df.groupby(['cryptocurrency']).apply(test_model)
    result.show()

    cmd = f'hdfs dfs -rmr {results_path}'
    os.system(cmd)
    result.write.csv(results_path, header=True)


if __name__ == '__main__':
    args = parser.parse_args()

    global use_sentiment
    use_sentiment = False
    if args.sentiment.lower() == 'true':
        use_sentiment = True

    split_date = datetime.strptime(args.split_date, '%m-%d-%Y').date()

    global model_path
    model_path = args.model_path

    global result_schema
    if use_sentiment:
        result_schema = StructType([
            StructField('ds', TimestampType()),
            StructField('y', FloatType()),
            StructField('cryptocurrency', StringType()),
            StructField('yhat', DoubleType()),
            StructField('yhat_upper', DoubleType()),
            StructField('yhat_lower', DoubleType()),
        ])
    else:
        result_schema = StructType([
            StructField('ds', TimestampType()),
            StructField('y', FloatType()),
            StructField('cryptocurrency', StringType()),
            StructField('sentiment', FloatType()),
            StructField('yhat', DoubleType()),
            StructField('yhat_upper', DoubleType()),
            StructField('yhat_lower', DoubleType()),
        ])

    main(args.table, args.start_date, args.results_path)
