select
    transaction_id
    , product_id
    , jumlah_pembelian as purchase_amount
    , jumlah_penjualan as sales_amount
    , tanggal_transaksi as transaction_date
from transaction