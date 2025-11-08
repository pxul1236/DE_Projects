import pandas as pd
import numpy as np

def transform_data(df):
    print("starting transformation..")
    df_trans = df.copy()

    #checking for missing values
    print("\nchecking for missing values...")
    missing_val = df_trans.isnull().sum()
    print(missing_val[missing_val > 0])

    if missing_val.sum() == 0:
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

    # Converting date columns
    print("\nConverting date columns...")
    df_trans['Order Date'] = pd.to_datetime(df_trans['Order Date'])
    df_trans['Ship Date'] = pd.to_datetime(df_trans['Ship Date'])

    # Extract date features
    print("\nExtracting date features...")
    df_trans['Order Year'] = df_trans['Order Date'].dt.year
    df_trans['Order Month'] = df_trans['Order Date'].dt.month
    df_trans['Order Day of Week'] = df_trans['Order Date'].dt.day_name()

    # Calculate business metrics
    print("\nCalculating business metrics...")
    df_trans['Profit Margin %'] = np.where( df_trans['Sales'] > 0,(df_trans['Profit'] / df_trans['Sales']) * 100, 0)
    
    df_trans['Shipping Days'] = (df_trans['Ship Date'] - df_trans['Order Date'] ).dt.days
    
    df_trans['Price per Unit'] = df_trans['Sales'] / df_trans['Quantity']

     # Clean text columns
    print("\nCleaning text columns...")
    text_columns = ['Customer Name', 'City', 'State', 'Product Name', 'Category', 'Sub-Category']
    for col in text_columns:
        df_trans[col] = df_trans[col].str.strip()
    
    #removing duplicate
    num_rows = len(df_trans)
    df_trans = df_trans.drop_duplicates()
    duplicates_rm = num_rows - len(df_trans)
    print(f"removed {duplicates_rm} duplicate rows")

    return df_trans

if __name__ == "__main__":
    from extract_data import extract_data

    raw_data = extract_data(r"data/store.csv")

    transformed_data = transform_data(raw_data)

    print(transformed_data.head())