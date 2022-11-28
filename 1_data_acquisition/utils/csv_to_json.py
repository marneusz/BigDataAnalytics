import argparse
import pandas as pd
import os

def csv_to_json(csv_path, json_path=None):
    df_dir = os.path.dirname(csv_path)
    df_name = os.path.splitext(os.path.basename(csv_path))[0]
    df = pd.read_csv(csv_path)
    if json_path is None:
        json_path = os.path.join(df_dir, f"{df_name}.json") 
        df.to_json(json_path, orient='records', lines=True)
    else:
        df.to_json(json_path, orient='records', lines=True)

    return json_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--csv-path', type=str, required=True)
    parser.add_argument('-pj', '--json-path', type=str, required=False, default=None)
    args = parser.parse_args()

    json_path = csv_to_json(args.csv_path, args.json_path)
    print(f"Saved JSON: {json_path}")
