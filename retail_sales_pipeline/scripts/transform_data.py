import pandas as pd
import numpy as np

df = pd.read_csv(r"data/store.csv")

def transform_data(df):
    print("starting transformation..")
    df_trans = df.copy()

    #checking for missing values
    print("\nchecking for missing values...")
    missing_val = df_trans.isnull().sum()
    print(missing_val[missing_val > 0])

    if missing_val == 0:
        print("No missing values found")
    else: 
        #missing values for numeric cols
        numeric_cols = df_trans.select_dtypes(include = [np.number]).columns
        for col in numeric_cols:
            if df_trans[col].isnull().sum() > 0:
                df_trans[col].fillna(df_trans[col].median(), inplace = True)
                print(f"Filled {col} with median")
        #missing values for categorical cols
        categorical_cols = df_trans.select_dtypes(include = ['object']).columns
        for col in categorical_cols:
            if df_trans[col].isnull().sum() > 0:
                df_trans[col].fillna('Unknown', inplace = True)
                print(f"Filled {col} with 'Unknown'")