import pandas as pd
import csv
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, Boolean, Float, Integer, DateTime

def get_data_warehouse():
    with open('..\Dataset\warehouselocation.csv', 'r') as f:
        reader = csv.reader(f)
        
        # Specify the column names
        headers = ['ID', 'Nama_gudang']

        # Create a new csv file with updated column names
        with open('warehouse_updated.csv', 'w', newline='') as f_updated:
            writer = csv.writer(f_updated)

            # Write the new column names to the csv file
            writer.writerow(headers)

            # Write the remaining rows from the original csv file
            for row in reader:
                writer.writerow(row)

    warehouse = pd.read_csv('warehouse_updated.csv')
    return warehouse

column_warehouse = get_data_warehouse()
print(column_warehouse.dtypes)

def get_manipulate_data(column_warehouse) :
    column_warehouse.dropna(inplace=True)
    column_warehouse['Nama_gudang'] = column_warehouse['Nama_gudang'].astype('string')
    return column_warehouse

clean_warehouse = get_manipulate_data(column_warehouse)
print(clean_warehouse.dtypes)

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
    warehouse_schema = {
        'ID':BigInteger,
        'Nama_gudang':String
        }

    column_warehouse.to_sql('Warehouse_Location', 
              con=engine, if_exists='replace',
              index=False, schema='public', 
              dtype=warehouse_schema, method=None, 
              chunksize=5000)

postgres_conn = get_postgres_conn()
# print(postgres_conn)

load_to_postgres(postgres_conn)

