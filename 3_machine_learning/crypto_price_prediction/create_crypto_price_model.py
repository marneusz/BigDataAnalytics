import argparse
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
from datetime import datetime

empty_schema = StructType([StructField('x', IntegerType())])

# th_date = datetime.datetime(2022, 12, 5)

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

# arguments

parser.add_argument('-s', '--sentiment', type=str, default='False')
parser.add_argument('-T', '--table', type=str, default='crypto_table')
parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/prophet_model')
parser.add_argument('-ds', '--data_split', type=str, default='False')
parser.add_argument('-d', '--end_date', type=str, default='2022-12-5')


@pandas_udf(empty_schema, PandasUDFType.GROUPED_MAP)
def fit_model(data):
    model = Prophet(
        interval_width=0.95,
        growth='linear',
        seasonality_mode='multiplicative'
    )
    if use_sentiment:
        model.add_regressor('sentiment', mode='multiplicative', prior_scale=100)

    model.fit(data)
    model_name_curr = f'prophet_model_{data.cryptocurrency[0]}.json'

    with open(model_name_curr, 'w+') as fout:
        fout.write(model_to_json(model))

    cmd = f'hdfs dfs -rm {model_path}/{model_name_curr}'
    os.system(cmd)
    cmd = f'hdfs dfs -put {model_name_curr} {model_path}/'
    os.system(cmd)
    os.remove(model_name_curr)

    return pd.DataFrame({'x': []})

def main(table, data_split, end_date):
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

    if data_split:
        train_df = spark.sql(f'{query_str} WHERE date<"{end_date.isoformat()}"')
    else:
        train_df = spark.sql(f'{query_str}')

    train_df = train_df.withColumn('ds', train_df.ds.cast('timestamp'))
    train_df = (train_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    empty_result = train_df.groupby(['cryptocurrency']).apply(fit_model)
    empty_result.show()

if __name__ == '__main__':
    args = parser.parse_args()

    global use_sentiment
    use_sentiment = False
    if args.sentiment.lower() == 'true':
        use_sentiment = True

    data_split = False
    if args.data_split.lower() == 'true':
        data_split = True

    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    global model_path
    model_path = args.model_path

    main(args.table, data_split, end_date)
