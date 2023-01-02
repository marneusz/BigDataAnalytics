import pandas as pd
import re
import argparse

parser = argparse.ArgumentParser(description='Preprocesses file with cryptocurrencies prices given its path')

# arguments
parser.add_argument('file_path', type=str)


def main(file_path):
    df = pd.read_csv(file_path)
    df["year"] = df.date.map(lambda x: re.split('-|:| ', x)[0])
    df["month"] = df.date.map(lambda x: re.split('-|:| ', x)[1])
    df["day"] = df.date.map(lambda x: re.split('-|:| ', x)[2])
    df["hour"] = df.date.map(lambda x: re.split('-|:| ', x)[3])

    file_path_out = f'{file_path.split(".")[-2]}_processed.csv'
    df.to_csv(file_path_out, index=False, header=False)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.file_path)
