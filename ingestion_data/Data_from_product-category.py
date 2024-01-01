import pandas as pd
import csv
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime

def get_data_product_category():
    with open('..\Dataset\product-category.csv', 'r') as f:
        reader = csv.reader(f)
        
        # Specify the column names
        headers = ['ID', 'Product_category']

        # Create a new csv file with updated column names
        with open('product_category_updated.csv', 'w', newline='') as f_updated:
            writer = csv.writer(f_updated)

            # Write the new column names to the csv file
            writer.writerow(headers)

            # Write the remaining rows from the original csv file
            for row in reader:
                writer.writerow(row)

    # Read the updated CSV file into a DataFrame
    product_ctg = pd.read_csv('product_category_updated.csv')
    return product_ctg

column_product_ctg = get_data_product_category()
print(column_product_ctg.dtypes)

def get_manipulate_data(column_product_ctg) :
    column_product_ctg.dropna(inplace=True)
    column_product_ctg['ID'] = column_product_ctg['ID'].astype('int8')
    column_product_ctg['Product_category'] = column_product_ctg['Product_category'].astype('string')
    return column_product_ctg

clean_produt_ctg = get_manipulate_data(column_product_ctg)
print(clean_produt_ctg.dtypes)

def get_postgres_conn():
    user = 'user'
    password = 'pass'
    host = 'localhost'
    database = 'ingestion_data'
    port = 5443
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string)
    return engine

def load_to_postgres(engine) :
    product_ctg_schema = {
        'ID':BigInteger,
        'Product_category':String
        }

    column_product_ctg.to_sql('Product_category', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=product_ctg_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

