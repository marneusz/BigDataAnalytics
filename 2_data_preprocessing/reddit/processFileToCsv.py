import json
import pandas as pd
import argparse
from datetime import datetime
import sys
sys.path.append('../utils')
import text_process

# Instantiate the parser
parser = argparse.ArgumentParser(description='Preprocesses given file (given file_path), chooses subreddits')

# arguments
parser.add_argument('file_path', type=str)

# reddit_subreddit_id : our_crypto_id
selected_subreddits = {'t5_2s3qj': 'bitcoin',
                       't5_2zf9m': 'ethereum',
                       't5_2zcp2': 'dogecoin',
                       't5_3jns3': 'cardano',
                       't5_2ruj5': 'xrp',
                       't5_hcs2n': 'solana'
                       }


def main(file_path):
    f = open(file_path, encoding="utf8")
    obs = []

    columns = ['title', 'selftext', 'created_utc']
    for line in f:
        curr = json.loads(line)
        if "subreddit_id" in curr and curr['subreddit_id'] in selected_subreddits.keys() \
                and curr['selftext'] not in ['[deleted]', '[removed]', '']:
            dict_you_want = {your_key: curr[your_key] for your_key in columns}
            dict_you_want['cryptocurrency'] = selected_subreddits[curr['subreddit_id']]
            obs.append(dict_you_want)

    df = pd.DataFrame(obs)
    # I don't get why we need this:
    df.replace({',': ''}, regex=True, inplace=True)

    text_normalizer = text_process.TextNormalizer()
    df["title"] = df.title.map(lambda x: text_normalizer.normalize(x))
    df["selftext"] = df.selftext.map(lambda x: text_normalizer.normalize(x))

    df["created_utc"] = df.created_utc.map(lambda x: int(x))
    df["year"] = df.created_utc.map(lambda x: datetime.utcfromtimestamp(x).year)
    df["month"] = df.created_utc.map(lambda x: datetime.utcfromtimestamp(x).month)
    df["day"] = df.created_utc.map(lambda x: datetime.utcfromtimestamp(x).day)

    with open(file_path + ".csv", mode='w', newline='\n') as f:
        df.to_csv(f, sep=",", float_format='%.2f',
                  index=False)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.file_path)
