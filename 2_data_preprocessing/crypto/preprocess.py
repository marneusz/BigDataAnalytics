import pandas as pd
import numpy as np


def main():
    df = pd.read_csv("crypto_historical_data.csv")
    df["year"] = df.date.map(lambda x: x.split("/")[2])
    df["month"] = df.date.map(lambda x: x.split("/")[1])
    df["day"] = df.date.map(lambda x: x.split("/")[0])

    df.to_csv("crypto_historical_data.csv", index = False,header = False)






if __name__ == "__main__":

    main()



