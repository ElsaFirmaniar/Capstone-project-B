    -- 1. Model untuk Menghitung Usia Produk (product_age.sql):
    --     - product_id
    --     - nama_produk
    --     - usia_hari
with product_age as (
    SELECT 
    product.product_id
    , product.nama_produk
    , (product.tanggal_kedaluwarsa - product.tanggal_produksi) AS usia_hari
from
    {{ source('dbt-project', 'product') }}

)

select * from product_age