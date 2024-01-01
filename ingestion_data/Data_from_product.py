import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime, DATE

def get_data_product () :
    product  = pd.read_parquet('../Dataset/product.parquet', engine="pyarrow")
    return product

product_type = get_data_product()
print(product_type.dtypes)
print(product_type)

column_product = product_type.rename(columns={'column0':'ID',
                                'column1':'Product_name',
                                'column2':'MFG_Date',
                                'column3':'EXP_Date',
                                'column4':'Product_ctg_id',
                                'column5':'Supplier_id'})

def get_manipulate_data(column_product) :
    column_product.dropna(inplace=True)
    column_product['Product_name'] = column_product['Product_name'].astype('string')
    column_product['MFG_Date'] = pd.to_datetime(column_product['MFG_Date']).dt.date
    column_product['EXP_Date'] = pd.to_datetime(column_product['EXP_Date']).dt.date
    # column_product['MFG_Date'] = column_product['MFG_Date'].astype('Date')
    # column_product['EXP_Date'] = column_product['EXP_Date'].astype('Date')
    return column_product

clean_product = get_manipulate_data(column_product)
print(clean_product.dtypes)
print(clean_product)

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
    product_schema = {
        'ID':BigInteger,
        'Product_name':String,
        'MFG_Date':DATE,
        'EXP_Date':DATE,
        'Product_ctg_id':BigInteger,
        'supplier_id':BigInteger
        }

    column_product.to_sql(name='Product', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=product_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

