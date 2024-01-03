    -- 4. Model untuk Statistik Kategori Produk (category_statistics.sql):
    --     - nama_kategori
    --     - jumlah_produk
    --     - rata_usia_produk

with category_statistics as (
    SELECT 
    productcategory.nama_kategori
    , count(product.category_id) AS jumlah_produk
    , AVG((product.tanggal_kedaluwarsa - product.tanggal_produksi)) AS rata_usia_produk
    FROM {{ source('dbt-project', 'product') }}
    RIGHT JOIN {{ source('dbt-project', 'productcategory') }}
    ON product.category_id = productcategory.category_id
    GROUP BY productcategory.nama_kategori
)
select 
*
from category_statistics