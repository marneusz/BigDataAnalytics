import os
import pandas as pd
import requests
import sys

from datetime import datetime
from hdfs import InsecureClient

crypto_ids = {
    "bitcoin",
    "ethereum",
    "dogecoin",
    "cardano",
    "ripple",
    "solana"
}


def fetch_data(crypto_id, out_dir, client):
    url = 'https://api.coingecko.com/api/v3/coins/' + crypto_id + '/market_chart?vs_currency=usd&days=max&interval=daily'
    req = requests.get(url)
    data = req.json()

    df = pd.DataFrame(data["prices"], columns=['date', 'price'])
    # dropping last row since API returns additionally most recent price from given day which is incoherent
    df = df[:-1]
    # convert unix timestamp to datetime
    df['date'] = df['date'].map(lambda x: datetime.utcfromtimestamp(x // 1000))
    # df.to_csv(os.path.join(out_dir, crypto_id + '_data.csv'), date_format="%d/%m/%Y", index=False)

    with client.write(os.path.join(out_dir, f'{crypto_id}_data.csv'), encoding='utf-8') as writer:
        df.to_csv(writer, date_format="%d/%m/%Y", index=False)


def test(crypto_id, out_dir):
    url = 'https://api.coingecko.com/api/v3/coins/' + crypto_id + '/market_chart?vs_currency=usd&days=max&interval=daily'
    req = requests.get(url)
    data = req.json()

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    df = pd.DataFrame(data["prices"], columns=['date', 'price'])
    # dropping last row since API returns additionally most recent price from given day which is incoherent
    df = df[:-1]
    # convert unix timestamp to datetime
    df['date'] = df['date'].map(lambda x: datetime.utcfromtimestamp(x // 1000))
    df.to_csv(os.path.join(out_dir, crypto_id + '_data.csv'), date_format="%d/%m/%Y", index=False)


def main():
    if "-o" in sys.argv:
        out_dir = sys.argv[sys.argv.index("-o") + 1]
    else:
        out_dir = "crypto_historical"

    if "-h" in sys.argv and "-u" in sys.argv:
        hostname = sys.argv[sys.argv.index("-h") + 1]
        username = sys.argv[sys.argv.index("-u") + 1]

        client = InsecureClient(hostname, username)

        for crypto_id in crypto_ids:
            fetch_data(crypto_id, out_dir, client)
    else:
        for crypto_id in crypto_ids:
            test(crypto_id, out_dir)


if __name__ == '__main__':
    main()
