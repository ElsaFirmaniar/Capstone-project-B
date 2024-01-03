-- 3. Model untuk Menganalisis Stok Tersedia (available_stock.sql):
--         - product_id
--         - nama_produk
--         - jumlah_stok
--         - lokasi_gudang

with available_stock as (
    SELECT 
    stock.product_id
    , product.nama_produk
    , stock.jumlah_stok
    , stock.lokasi_gudang
    FROM {{ source('dbt-project', 'stock') }}
    JOIN {{ source('dbt-project', 'product') }}
    ON product.product_id = stock.product_id
    ORDER BY stock.product_id
)

select * from available_stock
