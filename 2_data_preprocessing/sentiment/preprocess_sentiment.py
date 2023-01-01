import pandas as pd
import argparse
import sys
sys.path.append('../utils')
import text_process

parser = argparse.ArgumentParser(description='Preprocesses file with sentiment data given its path')

# arguments
parser.add_argument('file_path', type=str)


def main(file_path):
    df = pd.read_csv(file_path)
#     dff = df.copy()
    text_normalizer = text_process.TextNormalizer()
    df["Text"] = df.Text.map(lambda x: text_normalizer.normalize(x))

#     df_empty = dff[df.Text == ""]
#     df_empty.to_csv("deleted.csv", index=False)

    df = df[df.Text != ""]

    file_path_out = f'{file_path.split(".")[-2]}_processed.csv'
    df.to_csv(file_path_out, index=False)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.file_path)
