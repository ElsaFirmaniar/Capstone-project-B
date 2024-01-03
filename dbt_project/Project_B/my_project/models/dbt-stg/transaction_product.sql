-- 2. Model untuk Menggabungkan Data Transaksi dan Produk (transaction_product.sql):
--         - transaction_id
--         - jumlah_pembelian
--         - jumlah_penjualan
--         - tanggal_transaksi
--         - nama_produk
--         - tanggal_produksi

with transaction_product as (
    SELECT 
    transaction.transaction_id
    , transaction.jumlah_pembelian
    , transaction.jumlah_penjualan
    , transaction.tanggal_transaksi
    , product.nama_produk
    , product.tanggal_produksi
    FROM
        {{ source('dbt-project', 'transaction') }}
    JOIN
        {{ source('dbt-project', 'product') }}
    ON
        product.product_id = transaction.product_id
    ORDER BY transaction.transaction_id
)

select 
*
from transaction_product
