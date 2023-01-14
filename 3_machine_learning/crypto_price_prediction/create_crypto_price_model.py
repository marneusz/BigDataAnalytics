import pandas as pd
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
import datetime
import os

empty_schema = StructType([StructField('x', IntegerType())])

# th_date = datetime.datetime(2022, 12, 5)

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

# arguments

parser.add_argument('-s', '--sentiment', type=str, default='False')
parser.add_argument('-T', '--table', type=str, default='crypto_table')
parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/prophet_model')
parser.add_argument('-ts', '--test_split', type=str, default='False')
parser.add_argument('-d', '--split_date', type=str, default='2022-12-5')


@pandas_udf(empty_schema, PandasUDFType.GROUPED_MAP)
def fit_model(data):
    model = Prophet(
        interval_width=0.95,
        growth='linear',
        daily_seasonality=True,
        weekly_seasonality=False,
        yearly_seasonality=False,
        seasonality_mode='multiplicative'
    )
    if use_sentiment:
        model.add_regressor('senitment', standardize=False)

    model.fit(data)

    #     future_pd = model.make_future_dataframe(periods=16)
    #     future_pd = pd.merge(future_pd, past_pd[['ds', 'senitment']], on='ds', how='inner')
    #     future_pd = future_pd.fillna(method='ffill')

    #     future = model.predict(future_pd)

    model_name_curr = f'prophet_model_{data.cryptocurrency[0]}.json'

    # save model
    with open(model_name_curr, 'w+') as fout:
        fout.write(model_to_json(model))

    cmd = f'hdfs dfs -rm {model_path}/{model_name_curr}'
    os.system(cmd)
    cmd = f'hdfs dfs -put {model_name_curr} {model_path}/'
    os.system(cmd)
    os.remove(model_name_curr)

    return pd.DataFrame({'x': []})


#     f_pd = future[['ds', 'yhat', 'yhat_upper', 'yhat_lower']]
#     st_pd = data[['ds', 'cryptocurrency', 'y']]
#     result_pd = f_pd.join(st_pd.set_index('ds'), on='ds', how='left')

#     result_pd['cryptocurrency'] = data['cryptocurrency'].iloc[0]
#     return result_pd[['cryptocurrency', 'ds', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def test_model(data):
    model_path_curr = data.cryptocurrency[0] + '_' + model_path

    cmd = f'hdfs dfs -copyToLocal /user/bda_reddit_pw/models/{model_path_curr} .'
    os.system(cmd)
    with open(model_path_curr, 'r') as fin:
        model = model_from_json(fin.read())
    os.remove(model_path_curr)

    future = model.predict(data)

    f_pd = future[['ds', 'yhat', 'yhat_upper', 'yhat_lower']]
    st_pd = data[['ds', 'cryptocurrency', 'y']]
    result_pd = f_pd.join(st_pd.set_index('ds'), on='ds', how='left')

    result_pd['cryptocurrency'] = data['cryptocurrency'].iloc[0]
    return result_pd[['cryptocurrency', 'ds', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


def main(test_split, table, split_date):
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

    if test_split:
        train_df = spark.sql(
            f'{query_str} WHERE date<"{split_date.isoformat()}"')
        # test_df = spark.sql(
        #     f'{query_str} WHERE date>="{split_date.isoformat()}"')
    else:
        train_df = spark.sql(f'{query_str}')

    train_df = train_df.withColumn('ds', train_df.ds.cast('timestamp'))
    train_df = (train_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    # there should be probably something different from apply
    # becouse apply assumes we return some results and also works
    # only after show() but I don't know how to do it differently using pyspark
    empty_result = train_df.groupby(['cryptocurrency']).apply(fit_model)
    # the fit_model runs only after show()
    empty_result.show()

    # if test_split:
    #     test_df = test_df.withColumn('ds', test_df.ds.cast('timestamp'))
    #     test_df = (test_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    #     result = test_df.groupby(['cryptocurrency']).apply(test_model)
    #     result.show()
    #
    #     cmd = 'hdfs dfs -rmr /user/bda_reddit_pw/models/test_results/prophet_results'
    #     os.system(cmd)
    #     result.write.csv('/user/bda_reddit_pw/models/test_results/prophet_results', header=True)


if __name__ == '__main__':
    args = parser.parse_args()

    global use_sentiment
    use_sentiment = False
    if args.sentiment.lower() == 'true':
        use_sentiment = True

    test_split = False
    if args.test_split.lower() == 'true':
        test_split = True

    split_date = datetime.strptime(args.split_date, '%Y-%m-%d').date()

    global model_path
    model_path = args.model_path

    main(test_split, args.table, split_date)
