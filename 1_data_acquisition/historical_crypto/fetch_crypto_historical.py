import os
import pandas as pd
import requests
import sys
from datetime import datetime

crypto_ids = {
    "bitcoin",
    "ethereum",
    "dogecoin",
    "cardano",
    "ripple",
    "solana"
}


def fetch_data(out_dir):
    crypto_data = pd.DataFrame()

    for c in crypto_ids:
        url = 'https://api.coingecko.com/api/v3/coins/' + c + '/market_chart?vs_currency=usd&days=max&interval=daily'
        req = requests.get(url)
        data = req.json()

        df = pd.DataFrame(data["prices"], columns=['date', 'price'])
        df = df[:-1]

        df['date'] = df['date'].map(lambda x: datetime.utcfromtimestamp(x // 1000))
        df['subreddit'] = c

        crypto_data = pd.concat([crypto_data, df])

    crypto_data.to_csv(os.path.join(out_dir, 'crypto_historical_data.csv'), date_format="%d/%m/%Y", index=False)


def main():
    if "-o" in sys.argv:
        out_dir = sys.argv[sys.argv.index("-o") + 1]
    else:
        out_dir = "data"

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    fetch_data(out_dir)


if __name__ == '__main__':
    main()
