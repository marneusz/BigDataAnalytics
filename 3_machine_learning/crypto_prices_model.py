import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
import datetime
import os

empty_schema = StructType([StructField('x', IntegerType())])
result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('y', FloatType()),
    StructField('cryptocurrency', StringType()),
    StructField('yhat', DoubleType()),
    StructField('yhat_upper', DoubleType()),
    StructField('yhat_lower', DoubleType()),
])

model_path = 'prophet_model.json'
th_date = datetime.datetime(2022, 12, 5)


@pandas_udf(empty_schema, PandasUDFType.GROUPED_MAP)
def fit_model(data):
    model = Prophet(
      interval_width=0.95,
      growth='linear',
      seasonality_mode='multiplicative'
    )
#       daily_seasonality=False,
#       weekly_seasonality=True,
#       yearly_seasonality=True,
#     prophet_model.add_regressor('senitment', standardize=False)

    model.fit(data)

#     future_pd = model.make_future_dataframe(periods=16)
#     future_pd = pd.merge(future_pd, past_pd[['ds', 'senitment']], on='ds', how='inner')
#     future_pd = future_pd.fillna(method='ffill')

#     future = model.predict(future_pd)

    model_path_curr = data.cryptocurrency[0] + '_' + model_path

    # save model
    with open(model_path_curr, 'w+') as fout:
        fout.write(model_to_json(model))
    
    cmd = f'hdfs dfs -rm /user/bda_reddit_pw/models/{model_path_curr}'
    os.system(cmd)
    cmd = f'hdfs dfs -put {model_path_curr} /user/bda_reddit_pw/models/'
    os.system(cmd)
    os.remove(model_path_curr)

    return pd.DataFrame({'x' : []})

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


def main():
    spark = SparkSession.builder \
     .master('local[1]') \
     .appName('Crypto Price Prediction') \
     .enableHiveSupport() \
     .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    train_df = spark.sql(f'SELECT date as ds, price as y, cryptocurrency FROM crypto_table WHERE date<"{th_date.isoformat()}"')
    test_df = spark.sql(f'SELECT date as ds, price as y, cryptocurrency FROM crypto_table WHERE date>="{th_date.isoformat()}"')
    
    train_df = train_df.withColumn('ds', train_df.ds.cast('timestamp'))
    test_df = test_df.withColumn('ds', test_df.ds.cast('timestamp'))

#     train_df, test_df = df.randomSplit([0.7, 0.3])
    train_df = (train_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    test_df = (test_df.repartition(spark.sparkContext.defaultParallelism, ['cryptocurrency'])).cache()
    
    # there should be probably something different from apply
    # becouse apply assumes we return some results and also works
    # only after show() but I don't know how to do it differently using pyspark
    empty_result = train_df.groupby(['cryptocurrency']).apply(fit_model)
    # the fit_model runs only after show()
    empty_result.show()

    result = test_df.groupby(['cryptocurrency']).apply(test_model)
    result.show()

    cmd = 'hdfs dfs -rmr /user/bda_reddit_pw/models/test_results/prophet_results'
    os.system(cmd)
    result.write.csv('/user/bda_reddit_pw/models/test_results/prophet_results', header=True)

    # accuracy/mse
    # zapisać odczytać model
    # wykresy?

if __name__ == '__main__':
    main()
