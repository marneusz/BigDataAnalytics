import pandas as pd
import os

sentiment_reddit_url = 'https://raw.github.com/surge-ai/crypto-sentiment/main/sentiment.csv'
sentiment_twitter_url = 'https://raw.github.com/surge-ai/stock-sentiment/main/sentiment.csv'

out_dir = 'data'


def main():
    os.environ['KAGGLE_USERNAME'] = "bdared"
    os.environ['KAGGLE_KEY'] = "b47c5d207bf238ad6fbf29ba4b8cba58"

    os.system('kaggle datasets download -d yash612/stockmarket-sentiment-dataset')

    kaggle_dataset = pd.read_csv('./stockmarket-sentiment-dataset.zip', compression='zip')
    os.remove('stockmarket-sentiment-dataset.zip')

    sentiment_reddit = pd.read_csv(sentiment_reddit_url)
    sentiment_twitter = pd.read_csv(sentiment_twitter_url)

    sentiment_reddit = sentiment_reddit.iloc[:, [0, 1]]
    sentiment_twitter = sentiment_twitter.iloc[:, [1, 2]]

    sentiment_reddit.rename({'Comment Text': 'Text'}, axis=1, inplace=True)
    sentiment_twitter.rename({'Tweet Text': 'Text'}, axis=1, inplace=True)

    sentiment_data = pd.concat([sentiment_reddit, sentiment_twitter], axis=0)
    sentiment_data.replace({"Positive": True, "Negative": False}, inplace=True)
    kaggle_dataset.replace({1: True, -1: False}, inplace=True)
    sentiment_data = pd.concat([sentiment_data, kaggle_dataset], axis=0)

    sentiment_data.reset_index(inplace=True, drop=True)

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    sentiment_data.to_csv(os.path.join(out_dir, 'sentiment_data.csv'), index=False)


if __name__ == '__main__':
    main()
