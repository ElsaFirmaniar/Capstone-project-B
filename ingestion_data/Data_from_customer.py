import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime

def get_data_customer () :
    customer  = pd.read_parquet('../Dataset/Customer.parquet', engine="pyarrow")
    return customer

cust = get_data_customer()
print(cust.dtypes)

column_cust = cust.rename(columns={'column0':'id',
                                'column1':'customer',
                                'column2':'alamat'})

def get_manipulate_data(column_cust) :
    column_cust.dropna(inplace=True)
    column_cust['id'] = column_cust['id'].astype('int8')
    column_cust['customer'] = column_cust['customer'].astype('string')
    column_cust['alamat'] = column_cust['alamat'].astype('string')
    return column_cust

clean_cust = get_manipulate_data(column_cust)
print(clean_cust.dtypes)

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
    cust_schema = {
        'id':BigInteger,
        'customer':String,
        'alamat':String
        }

    column_cust.to_sql(name='Customer', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=cust_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

