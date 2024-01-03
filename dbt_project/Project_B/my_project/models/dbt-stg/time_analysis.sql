-- 5. Model untuk Menganalisis Data Waktu (time_analysis.sql):
--         - tanggal
--         - jumlah_transaksi

with time_analysis as (
    SELECT 
    transaction.tanggal_transaksi
    , transaction.jumlah_penjualan
    FROM {{ source('dbt-project', 'transaction') }}
)

select * from time_analysis
