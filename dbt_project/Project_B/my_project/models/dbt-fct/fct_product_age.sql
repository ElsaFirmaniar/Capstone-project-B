    -- 1. Model untuk Menghitung Usia Produk (product_age.sql):
    --     - product_id
    --     - nama_produk
    --     - usia_hari

SELECT 
    product_id
    , product_name as nama_produk
    , (expired_date - production_date) AS usia_hari
from
    {{ ref('stg_product') }} 