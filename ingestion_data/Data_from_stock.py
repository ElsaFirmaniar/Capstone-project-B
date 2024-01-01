import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime

def get_data_stock () :
    stock  = pd.read_parquet('../Dataset/stock.parquet', engine="pyarrow")
    return stock

stock_type = get_data_stock()
print(stock_type.dtypes)
print(stock_type)

column_stock = stock_type.rename(columns={'column0':'ID',
                                'column1':'Product_id',
                                'column2':'Stock',
                                'column3':'Lokasi_gudang'})

def get_manipulate_data(column_stock) :
    column_stock.dropna(inplace=True)
    column_stock['Lokasi_gudang'] = column_stock['Lokasi_gudang'].astype('string')
    return column_stock

clean_stock = get_manipulate_data(column_stock)
print(clean_stock.dtypes)

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
    stock_schema = {
        'ID':BigInteger,
        'Product_id':BigInteger,
        'Stock':BigInteger,
        'Lokasi_gudang':String
        }

    column_stock.to_sql(name='Stock', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=stock_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

