import re
import nltk
import pandas as pd

def clean_text(df, text):

    df["title"] = df.title.map(lambda x: clean_text(x, words))
    df["selftext"] = df.selftext.map(lambda x: clean_text(x, words))

    nltk.download('words')
    words = set(nltk.corpus.words.words())

    my_str = " ".join(w for w in nltk.wordpunct_tokenize(text) \
                      if w.lower() in words or not w.isalpha())
    my_str = re.sub(r'[^a-zA-Z\s]', '', my_str)
    my_str = re.sub("\s\s+", " ", my_str)
    return my_str.strip()
