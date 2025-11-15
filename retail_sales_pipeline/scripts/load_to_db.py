import pandas as pd
import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'retail_sales',
    'user': 'postgres',
    'password': 'Paulcj103sql'
}

def create_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"Error connecting: {e}")
        return None

def create_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales_data (
        row_id INTEGER PRIMARY KEY,
        order_id VARCHAR(50),
        order_date DATE,
        ship_date DATE,
        ship_mode VARCHAR(50),
        customer_id VARCHAR(50),
        customer_name VARCHAR(100),
        segment VARCHAR(50),
        country VARCHAR(50),
        city VARCHAR(50),
        state VARCHAR(50),
        postal_code INTEGER,
        region VARCHAR(50),
        product_id VARCHAR(50),
        category VARCHAR(50),
        sub_category VARCHAR(50),
        product_name VARCHAR(200),
        sales DECIMAL(10,2),
        quantity INTEGER,
        discount DECIMAL(5,2),
        profit DECIMAL(10,2),
        order_year INTEGER,
        order_month INTEGER,
        order_day_of_week VARCHAR(20),
        profit_margin_percent DECIMAL(10,2),
        shipping_days INTEGER,
        price_per_unit DECIMAL(10,2)
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table created successfully")
        cursor.close()
    except Exception as e:
        print(f"Error creating table: {e}")

def load_data(conn, df):
    try:
        cursor = conn.cursor()
        
        # Prepare column names
        columns = ['row_id', 'order_id', 'order_date', 'ship_date', 'ship_mode',
                   'customer_id', 'customer_name', 'segment', 'country', 'city',
                   'state', 'postal_code', 'region', 'product_id', 'category',
                   'sub_category', 'product_name', 'sales', 'quantity', 'discount',
                   'profit', 'order_year', 'order_month', 'order_day_of_week',
                   'profit_margin_percent', 'shipping_days', 'price_per_unit']
        
        # Map DataFrame columns to database columns
        df_renamed = df.rename(columns={
            'Row ID': 'row_id',
            'Order ID': 'order_id',
            'Order Date': 'order_date',
            'Ship Date': 'ship_date',
            'Ship Mode': 'ship_mode',
            'Customer ID': 'customer_id',
            'Customer Name': 'customer_name',
            'Segment': 'segment',
            'Country': 'country',
            'City': 'city',
            'State': 'state',
            'Postal Code': 'postal_code',
            'Region': 'region',
            'Product ID': 'product_id',
            'Category': 'category',
            'Sub-Category': 'sub_category',
            'Product Name': 'product_name',
            'Sales': 'sales',
            'Quantity': 'quantity',
            'Discount': 'discount',
            'Profit': 'profit',
            'Order Year': 'order_year',
            'Order Month': 'order_month',
            'Order Day of Week': 'order_day_of_week',
            'Profit Margin %': 'profit_margin_percent',
            'Shipping Days': 'shipping_days',
            'Price per Unit': 'price_per_unit'
        })
        
        # Select only needed columns
        df_to_load = df_renamed[columns]
        
        # Insert data
        for index, row in df_to_load.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO sales_data ({})
                VALUES ({})
                ON CONFLICT (row_id) DO NOTHING
            """).format(
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            )
            cursor.execute(insert_query, tuple(row))
        
        conn.commit()
        print(f"Loaded {len(df_to_load)} rows successfully")
        cursor.close()
        
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()

if __name__ == "__main__":
    from extract_data import extract_data
    from transform_data import transform_data
    
    print("Starting ETL pipeline...")
    
    raw_data = extract_data(r"data/store.csv")
    transformed_data = transform_data(raw_data)
    
    conn = create_connection()
    if conn:
        create_table(conn)
        load_data(conn, transformed_data)
        conn.close()
        print("ETL pipeline complete")