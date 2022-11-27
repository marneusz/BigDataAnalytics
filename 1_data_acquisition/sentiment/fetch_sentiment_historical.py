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

    sentiment_reddit.to_csv(os.path.join(out_dir, 'sentiment_reddit.csv'), index=False)
    sentiment_twitter.to_csv(os.path.join(out_dir, 'sentiment_twitter.csv'), index=False)


if __name__ == '__main__':
    main()
