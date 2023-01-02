import argparse
import os
import pandas as pd
import requests
from datetime import datetime

# coingecko_crypto_id : our_crypto_id
selected_crypto = {
    'bitcoin': 'bitcoin',
    'ethereum': 'ethereum',
    'dogecoin': 'dogecoin',
    'cardano': 'cardano',
    'ripple': 'xrp',
    'solana': 'solana'
}

parser = argparse.ArgumentParser(description='Downloads historical prices of the cryptocurrencies')

# arguments
parser.add_argument('-d', '--days', type=int, default=90)
parser.add_argument('-o', '--out', type=str, default='')


def fetch_data(out_dir, days):
    if out_dir and not os.path.exists(out_dir):
        os.mkdir(out_dir)

    crypto_data = pd.DataFrame()

    for crypto_id in selected_crypto.keys():
        if days <= 0:
            break
        if days <= 90:
            # hourly - only last 90 days
            url = f'https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart?vs_currency=usd&days={days}&interval=hourly'
        else:
            # daily - all time
            url = f'https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart?vs_currency=usd&days={days}&interval=daily'
        req = requests.get(url)
        data = req.json()

        df = pd.DataFrame(data["prices"], columns=['date', 'price'])
        # dropping last row since API returns additionally most recent price from given day which is incoherent
        df = df[:-1]
        # convert unix timestamp to datetime
        df['date'] = df['date'].map(lambda x: datetime.utcfromtimestamp(x // 1000))

        df['cryptocurrency'] = selected_crypto[crypto_id]

        crypto_data = pd.concat([crypto_data, df])

    crypto_data.to_csv(os.path.join(out_dir, 'historical_crypto_data.csv'), index=False)


def main():
    args = parser.parse_args()
    fetch_data(args.out, args.days)


if __name__ == '__main__':
    main()
