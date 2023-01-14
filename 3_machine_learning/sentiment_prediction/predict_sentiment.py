from pyspark.ml import PipelineModel
import argparse
import os

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

# arguments

parser.add_argument('-T', '--table', type=str, default='reddit_table')
parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/sentiment_model')
parser.add_argument('-r', '--result_path', type=str, default='/user/bda_reddit_pw/models/results/prophet_results')


def main(table, model_path, results_path):
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('Sentiment Prediction') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    loaded_model = PipelineModel.load(model_path)

    test_df = spark.sql(f'SELECT * FROM {table}')
    prediction = loaded_model.transform(test_df)

    test_df['prediction'] = prediction

    spark.sparkContext.setLogLevel('ERROR')

    cmd = f'hdfs dfs -rmr {results_path}'
    os.system(cmd)
    test_df.write.csv(results_path, header=True)


if __name__ == '__main__':
    args = parser.parse_args()
    main(args.table, args.model_path, args.results_path)
