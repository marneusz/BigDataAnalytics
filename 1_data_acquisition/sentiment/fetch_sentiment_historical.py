import pandas as pd
import os

sentiment_reddit_url = 'https://raw.github.com/surge-ai/crypto-sentiment/main/sentiment.csv'
sentiment_twitter_url = 'https://raw.github.com/surge-ai/stock-sentiment/main/sentiment.csv'

out_dir = 'data'


def main():
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    sentiment_reddit = pd.read_csv(sentiment_reddit_url)
    sentiment_twitter = pd.read_csv(sentiment_twitter_url)

    sentiment_reddit = sentiment_reddit.iloc[:, [0, 1]]
    sentiment_twitter = sentiment_twitter.iloc[:, [1, 2]]

    sentiment_reddit.rename({'Comment Text': 'Subreddit'}, axis=1, inplace=True)
    sentiment_twitter.rename({'Tweet Text': 'Subreddit'}, axis=1, inplace=True)

    sentiment_data = pd.concat([sentiment_reddit, sentiment_twitter], axis=0)
    sentiment_data.reset_index(inplace=True, drop=True)

    sentiment_data.to_csv(os.path.join(out_dir, 'sentiment_data.csv'), index=False)


if __name__ == '__main__':
    main()
