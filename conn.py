import psycopg2
from datetime import datetime


# Koneksi ke database sumber
source_conn = psycopg2.connect(
    host='localhost',
    port='5443',
    user='user',
    password='pass',
    database='ingestion_data'
)

# Koneksi ke database tujuan
target_conn = psycopg2.connect(
    host='localhost',
    port='5433',
    user='postgres',
    password='pass',
    database='dbt-project'
)

# Membuat kursor untuk database sumber
source_cursor = source_conn.cursor()

# Eksekusi query SELECT pada database sumber
source_cursor.execute('SELECT * FROM "Customer"')

# Ambil hasil query
records = source_cursor.fetchall()

# Membuat kursor untuk database tujuan
target_cursor = target_conn.cursor()

# Eksekusi query INSERT pada database tujuan
for record in records:
    # Mengubah tipe data placeholder sesuai dengan tipe data kolom di "Product" table
    target_cursor.execute('INSERT INTO customer VALUES (%s, %s,%s)', 
                          (int(record[0]), str(record[1]), str(record[2])))

# Commit perubahan pada database tujuan
target_conn.commit()

# Tutup kursor dan koneksi
source_cursor.close()
target_cursor.close()
source_conn.close()
target_conn.close()
