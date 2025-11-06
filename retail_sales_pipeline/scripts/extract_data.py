import pandas as pd
import os

def extract_data(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    else:
        print(f"Reading data from: {file_path} ")

        df = pd.read_csv(file_path)

        if df.empty:
            raise ValueError("CSV file is empty!")
        else: 
            print(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            print(f"\n First 5 rows: ")
            print(df.head())

    return df

if __name__ == "__main__":
    file_path = r"data\Sample - Superstore.csv"
    data = extract_data(file_path)

    print(f"\nData Info: ")
    print(data.info())