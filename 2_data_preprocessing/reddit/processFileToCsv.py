import json
import pandas as pd
import argparse
from datetime import datetime
import nltk
import re


# Instantiate the parser
parser = argparse.ArgumentParser(description='Preprocesses given file ( given file_path), chooses subreddits')

# arguments
parser.add_argument('file_path', type=str)





def main(file_path):
    nltk.download('words')
    words = set(nltk.corpus.words.words())
    
    
    selected_subreddits = ['bitcoin',
                       'ethereum',
                       'dogecoin',
                       'cardano',
                       'XRP',
                       'solana'
                       ]


    f = open(file_path)
    obs = []
    # columns = ['subreddit', 'link_flair_text', 'title', 'selftext', 'score', 'url', 
    #            'num_comments', 'created_utc', 'is_self']
    columns = ['subreddit', 'title', 'selftext', 'created_utc']
    for line in f:
        curr = json.loads(line)
        # print(curr)
        if 'subreddit' not in curr:
            if "subreddit_id" in curr and curr["subreddit_id"] == 't5_2s3qj':
                curr["subreddit"] = 'bitcoin'
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
    
    with open(file_path + ".csv", mode='w', newline='\n') as f:
            df.to_csv(f, sep=",", float_format='%.2f',
                              index=False)


def clean_text(text, words):
    my_str = " ".join(w for w in nltk.wordpunct_tokenize(text) \
         if w.lower() in words or not w.isalpha())
    my_str = re.sub(r'[^a-zA-Z\s]', '', my_str)
    my_str = re.sub("\s\s+" , " ", my_str)
    return my_str.strip()


if __name__ == "__main__":

    args = parser.parse_args()

    main(args.file_path)


