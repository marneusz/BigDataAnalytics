from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

parser.add_argument('-T', '--table', type=str, default='sentiment_table')
parser.add_argument('-m', '--model_path', type=str, default='/user/bda_reddit_pw/models/sentiment_model')
parser.add_argument('-ts', '--test_split', type=str, default='False')


def main(table, model_path, test_split):
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('Sentiment Prediction') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    df = spark.sql(f'SELECT * FROM {table}')

    df = df.na.drop()
    df = df.na.replace(-1, 0)
    df = df.withColumn("sentiment", df.sentiment.cast('double'))

    if test_split:
        train_df, test_df = df.randomSplit([0.7, 0.3])
    else:
        train_df = df

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.001, labelCol='sentiment')
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

    model = pipeline.fit(train_df)
    model.save(model_path)

    if test_split:
        prediction = model.transform(test_df)
        eval = BinaryClassificationEvaluator(labelCol="sentiment")
        print(f'Model Accuracy: {eval.evaluate(prediction)}')


if __name__ == '__main__':
    args = parser.parse_args()

    test_split = False
    if args.test_split.lower() == 'true':
        test_split = True

    main(args.table, args.model_path, test_split)
