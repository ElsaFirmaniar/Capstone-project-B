-- 5. Model untuk Menganalisis Data Waktu (time_analysis.sql):
--         - tanggal
--         - jumlah_transaksi


select    
    transaction_date as tanggal
    , sales_amount as jumlah_transaksi
from
    {{ ref('stg_transaction') }} 