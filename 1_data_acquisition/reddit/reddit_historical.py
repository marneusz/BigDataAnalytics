import numpy as np
import json
import os

import argparse

# Instantiate the parser
parser = argparse.ArgumentParser(description='Downloads data from reddit submissions page')

# arguments
parser.add_argument('from_year', type=int)
parser.add_argument('to_year', type=int)
parser.add_argument('from_month', type=int)
parser.add_argument('to_month', type=int)
parser.add_argument('subreddits_only', type=str)

# reddit_subreddit_id : reddit_subreddit_display_name (currently not used in code)
selected_subreddits = {'t5_2s3qj': 'Bitcoin',
                       't5_2zf9m': 'ethereum',
                       't5_2zcp2': 'dogecoin',
                       't5_3jns3': 'cardano',
                       't5_2ruj5': 'XRP',
                       't5_hcs2n': 'solana',
                       't5_2szgd': 'litecoin',
                       't5_3k6uh': 'algorand',
                       't5_2qgijx': '0xPolygon'
                       }


def main(from_year, to_year, from_month, to_month, subreddits_only):
    download_links = get_submission_links(from_year=from_year, to_year=to_year, from_month=from_month,
                                          to_month=to_month)

    for link in download_links:
        file_name = link.split("/")[-1]
        cmd = "curl --output {1} {0}".format(link, file_name)

        os.system(cmd)

        cmd = "zstd -d --long=31 {0}".format(file_name)
        os.system(cmd)

        os.remove(file_name)

        if subreddits_only:
            filter_subreddits(file_name.split('.zst')[0])


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
                submission_links.append('https://files.pushshift.io/reddit/submissions/RS_' + str(year) + '-' + str(
                    np.where(month < 10, '0' + str(month), str(month))) + '.zst')

            continue

        if year < to_year:
            for month in range(actual_month, 12 + 1):
                submission_links.append('https://files.pushshift.io/reddit/submissions/RS_' + str(year) + '-' + str(
                    np.where(month < 10, '0' + str(month), str(month))) + '.zst')

            actual_month = 1

    return submission_links


def filter_subreddits(file_path):
    with open(file_path, encoding="utf8") as f_in:
        with open(f'{file_path}_sub', encoding="utf8", mode='w+') as f_out:
            for line in f_in:
                curr = json.loads(line)
                # Apparently checking only the id works exactly the same as checking with subreddit
                if "subreddit_id" in curr and curr['subreddit_id'] in selected_subreddits.keys():
                    f_out.write(line)
    os.remove(file_path)


if __name__ == "__main__":
    args = parser.parse_args()
    # Doing it another way causes problems
    subreddits_only = False
    if args.subreddits_only.lower() == 'true':
        subreddits_only = True
    main(args.from_year, args.to_year, args.from_month, args.to_month, subreddits_only)
