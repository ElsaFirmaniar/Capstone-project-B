import pandas as pd
import csv
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime, DATE

def get_data_time():
    with open('..\\Dataset\\timedimension.csv', 'r') as f:
        reader = csv.reader(f)
        
        # Specify the column names
        headers = ['ID', 'Tanggal_produksi', 'Bulan', 'Tahun']

        # Create a new csv file with updated column names
        with open('timedimension_updated.csv', 'w', newline='') as f_updated:
            writer = csv.writer(f_updated)

            # Write the new column names to the csv file
            writer.writerow(headers)

            # Write the remaining rows from the original csv file
            for row in reader:
                writer.writerow(row)

    time = pd.read_csv('timedimension_updated.csv')
    return time

column_time = get_data_time()
print(column_time.dtypes)

# def get_manipulate_data(column_supplier) :
#     column_supplier.dropna(inplace=True)
#     column_supplier['Supplier_name'] = column_supplier['Supplier_name'].astype('string')
#     return column_supplier

# clean_supplier = get_manipulate_data(column_supplier)
# print(clean_supplier.dtypes)

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
    time_schema = {
        'ID':BigInteger,
        'Tanggal_produksi':DATE,
        'Bulan':BigInteger,
        'Tahun':BigInteger
        }

    column_time.to_sql('Timedimension', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=time_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

