-- 2. Model untuk Menggabungkan Data Transaksi dan Produk (transaction_product.sql):
--         - transaction_id
--         - jumlah_pembelian
--         - jumlah_penjualan
--         - tanggal_transaksi
--         - nama_produk
--         - tanggal_produksi
SELECT 
    transaction_id
    , purchase_amount as jumlah_pembelian
    , sales_amount as jumlah_penjualan
    , transaction_date as tanggal_transaksi
    , product_name as nama_produk
    , production_date as tanggal_produksi
from
    {{ ref('stg_transaction') }} stg_transaction
LEFT JOIN
    {{ ref('stg_product') }} stg_product
ON
    stg_product.product_id = stg_transaction.product_id
ORDER BY transaction_id

