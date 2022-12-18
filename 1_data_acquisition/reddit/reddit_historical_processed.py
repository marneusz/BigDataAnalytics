import numpy as np
import json
import pandas as pd
from datetime import datetime
import nltk
import re
import os

import argparse

# Instantiate the parser
parser = argparse.ArgumentParser(description='Downloads data from reddit submissions page')

# arguments
parser.add_argument('from_year', type=int)
parser.add_argument('to_year', type=int)
parser.add_argument('from_month', type=int)
parser.add_argument('to_month', type=int)


def main(from_year, to_year, from_month, to_month):
    download_links = get_submission_links(from_year=from_year, to_year=to_year, from_month=from_month,
                                          to_month=to_month)

    for link in download_links:
        file_name = link.split("/")[-1]
        cmd = "curl --output {1} {0}".format(link, file_name)

        os.system(cmd)

        cmd = "zstd -d --long=31 {0}".format(file_name)
        os.system(cmd)

        os.remove(file_name)

        filter_text(file_name.split('.zst')[0])


# from/to_year , from/to_month - integers
def get_submission_links(from_year, to_year, from_month, to_month):
    if from_year <= 2000 or to_year <= 2000 or from_month <= 0 or to_month <= 0 or from_month >= 13 or to_month >= 13:
        raise Exception("invalid year or month value")

    if to_year < from_year:
        raise Exception("invalid years interval: to_year < from_year")
    if to_year == from_year and to_month < from_month:
        raise Exception("invalid months interval: to_month < from_month for the same year")

    submission_links = []

    actual_month = from_month

    for year in range(from_year, to_year + 1):

        # same year
        if year == to_year:

            for month in range(actual_month, to_month + 1):
                #             print(str(year) + '  ' + str(month)  )
                submission_links.append('https://files.pushshift.io/reddit/submissions/RS_' + str(year) + '-' + str(
                    np.where(month < 10, '0' + str(month), str(month))) + '.zst')

            continue

        if year < to_year:

            for month in range(actual_month, 12 + 1):
                #             print(str(year) + '  ' + str(month)  )
                submission_links.append('https://files.pushshift.io/reddit/submissions/RS_' + str(year) + '-' + str(
                    np.where(month < 10, '0' + str(month), str(month))) + '.zst')

            actual_month = 1

    return submission_links


def filter_text(file_path):
    nltk.download('words')
    words = set(nltk.corpus.words.words())

    selected_subreddits = ['Bitcoin',
                           'ethereum',
                           'dogecoin',
                           'cardano',
                           'XRP',
                           'solana'
                           ]

    f = open(file_path, encoding="utf8")
    obs = []
    # columns = ['subreddit', 'link_flair_text', 'title', 'selftext', 'score', 'url',
    #            'num_comments', 'created_utc', 'is_self']
    columns = ['subreddit', 'title', 'selftext', 'created_utc']
    for line in f:
        curr = json.loads(line)
        # print(curr)
        if 'subreddit' not in curr:
            if "subreddit_id" in curr and curr["subreddit_id"] == 't5_2s3qj':
                curr["subreddit"] = 'Bitcoin'
            elif "subreddit_id" in curr and curr["subreddit_id"] == 't5_2zf9m':
                curr["subreddit"] = 'ethereum'
            elif "subreddit_id" in curr and curr["subreddit_id"] == 't5_2zcp2':
                curr["subreddit"] = 'dogecoin'
            elif "subreddit_id" in curr and curr["subreddit_id"] == 't5_3jns3':
                curr["subreddit"] = 'cardano'
            elif "subreddit_id" in curr and curr["subreddit_id"] == 't5_2ruj5':
                curr["subreddit"] = 'XRP'
            elif "subreddit_id" in curr and curr["subreddit_id"] == 't5_hcs2n':
                curr["subreddit"] = 'solana'

            else:
                continue
        if curr['selftext'] not in ['[deleted]', '[removed]', ''] and curr['subreddit'] in selected_subreddits:
            dict_you_want = {your_key: curr[your_key] for your_key in columns}
            obs.append(dict_you_want)

    df = pd.DataFrame(obs)
    df.replace({',': ''}, regex=True, inplace=True)

    df["title"] = df.title.map(lambda x: clean_text(x, words))
    df["selftext"] = df.selftext.map(lambda x: clean_text(x, words))

    df["created_utc"] = df.created_utc.map(lambda x: int(x))
    df["year"] = df.created_utc.map(lambda x: datetime.utcfromtimestamp(x).year)
    df["month"] = df.created_utc.map(lambda x: datetime.utcfromtimestamp(x).month)
    df["day"] = df.created_utc.map(lambda x: datetime.utcfromtimestamp(x).day)

    with open(file_path + ".csv", mode='w', newline='\n', encoding="utf8") as f:
        df.to_csv(f, sep=",", float_format='%.2f',
                  index=False)
    os.remove(file_path)


def clean_text(text, words):
    my_str = " ".join(w for w in nltk.wordpunct_tokenize(text) \
                      if w.lower() in words or not w.isalpha())
    my_str = re.sub(r'[^a-zA-Z\s]', '', my_str)
    my_str = re.sub("\s\s+", " ", my_str)
    return my_str.strip()


if __name__ == "__main__":
    args = parser.parse_args()

    main(args.from_year, args.to_year, args.from_month, args.to_month)
