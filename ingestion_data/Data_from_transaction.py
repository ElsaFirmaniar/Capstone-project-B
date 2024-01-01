import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime, DATE

def get_data_transaction () :
    transaction  = pd.read_parquet('../Dataset/transaction.parquet', engine="pyarrow")
    return transaction

transaction_type = get_data_transaction()
print(transaction_type.dtypes)
print(transaction_type)

column_transaction = transaction_type.rename(columns={'column0':'ID',
                                'column1':'Customer_id',
                                'column2':'Jumlah_pembelian',
                                'column3':'Jumlah_transaksi',
                                'column4':'Tanggal_transaksi'})

# def get_manipulate_data(column_stock) :
#     column_stock.dropna(inplace=True)
#     column_stock['Lokasi_gudang'] = column_stock['Lokasi_gudang'].astype('string')
#     return column_stock

# clean_stock = get_manipulate_data(column_stock)
# print(clean_stock.dtypes)

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
    transaction_schema = {
        'ID':BigInteger,
        'Customer_id':BigInteger,
        'Jumlah_penjualan':BigInteger,
        'Jumlah_transaksi':BigInteger,
        'Tanggal_transaksi':DATE
        }

    column_transaction.to_sql(name='Transaction', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=transaction_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

