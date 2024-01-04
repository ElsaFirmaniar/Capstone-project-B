-- 3. Model untuk Menganalisis Stok Tersedia (available_stock.sql):
--         - product_id
--         - nama_produk
--         - jumlah_stok
--         - lokasi_gudang

SELECT 
    stg_product.product_id
    , product_name as nama_produk
    , stock_amount as jumlah_stok
    , warehouse_loc as lokasi_gudang
FROM {{ ref('stg_stock') }} stg_stock
LEFT JOIN {{ ref('stg_product') }} stg_product
ON stg_product.product_id = stg_stock.product_id
ORDER BY product_id

    