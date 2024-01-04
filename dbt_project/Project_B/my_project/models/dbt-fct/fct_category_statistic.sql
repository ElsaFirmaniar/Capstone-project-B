    -- 4. Model untuk Statistik Kategori Produk (category_statistics.sql):
    --     - nama_kategori
    --     - jumlah_produk
    --     - rata_usia_produk


SELECT 
    category_name as nama_kategori
    , COALESCE(count(stg_product.category_id), 0) AS jumlah_produk
    , COALESCE(AVG((expired_date - production_date)), 0) AS rata_usia_produk
FROM 
    {{ ref('stg_product') }} stg_product
RIGHT JOIN 
    {{ ref('stg_productcategory') }} stg_productcategory
ON stg_product.category_id = stg_productcategory.category_id
GROUP BY nama_kategori