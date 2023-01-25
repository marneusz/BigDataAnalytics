import pandas as pd
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
from datetime import datetime


parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

# arguments

parser.add_argument('-s', '--sentiment', type=str, default='False')
parser.add_argument('-T', '--table', type=str, default='crypto_table')
parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/prophet_model')
parser.add_argument('-r', '--results_path', type=str, default='/user/bda_reddit_pw/models/results/prophet_results')
parser.add_argument('-ds', '--data_split', type=str, default='False')
parser.add_argument('-d', '--start_date', type=str, default=datetime.now().strftime('%Y-%m-%d'))

result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('cryptocurrency', StringType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType()),
])

result_schema_sent = StructType([
    StructField('ds', TimestampType()),
    StructField('cryptocurrency', StringType()),
    StructField('sentiment', FloatType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType()),
])

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
    st_pd = data[['ds', 'cryptocurrency']]

    result_pd = f_pd.join(st_pd.set_index('ds'), on='ds', how='left')
    result_pd['cryptocurrency'] = data['cryptocurrency'].iloc[0]

    return result_pd[['cryptocurrency', 'ds', 'yhat', 'yhat_upper', 'yhat_lower']]

@pandas_udf(result_schema_sent, PandasUDFType.GROUPED_MAP)
def test_model_sent(data):
    model_name_curr = f'prophet_model_{data.cryptocurrency[0]}.json'

    cmd = f'hdfs dfs -copyToLocal {model_path}/{model_name_curr} .'
    os.system(cmd)
    with open(model_name_curr, 'r') as fin:
        model = model_from_json(fin.read())
    os.remove(model_name_curr)

    future = model.predict(data)

    f_pd = future[['ds', 'yhat', 'yhat_upper', 'yhat_lower']]
    st_pd = data[['ds', 'cryptocurrency', 'sentiment']]

    result_pd = f_pd.join(st_pd.set_index('ds'), on='ds', how='left')
    result_pd['cryptocurrency'] = data['cryptocurrency'].iloc[0]

    return result_pd[['cryptocurrency', 'sentiment', 'ds', 'yhat', 'yhat_upper', 'yhat_lower']]


def main(table, data_split, start_date, results_path, use_sentiment):
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('Crypto Price Prediction') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    if use_sentiment:
        query_str = f'SELECT date as ds, cryptocurrency, sentiment FROM {table}'
    else:
        query_str = f'SELECT date as ds, cryptocurrency FROM {table}'

    if data_split:
        test_df = spark.sql(f'{query_str} WHERE date>="{start_date.isoformat()}"')
    else:
        test_df = spark.sql(f'{query_str}')
    
    test_df = test_df.withColumn('ds', test_df.ds.cast('timestamp'))
    test_df = (test_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    if use_sentiment:
        result = test_df.groupby(['cryptocurrency']).apply(test_model_sent)
    else:
        result = test_df.groupby(['cryptocurrency']).apply(test_model)
    result.show()

    cmd = f'hdfs dfs -rm -r {results_path}'
    os.system(cmd)
    result.write.csv(results_path, header=True)


if __name__ == '__main__':
    args = parser.parse_args()

    global use_sentiment
    use_sentiment = False
    if args.sentiment.lower() == 'true':
        use_sentiment = True

    data_split = False
    if args.data_split.lower() == 'true':
        data_split = True

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()

    global model_path
    model_path = args.model_path

    main(args.table, data_split, start_date, args.results_path, use_sentiment)
