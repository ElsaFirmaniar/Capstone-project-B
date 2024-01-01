# Project Brief Template Data Engineering

## 💻 Technical Brief

## Contraints

- Data terpisah berasal dari multiple source seperti db, excel, dan data source yg lain.
- Constraint setiap problem akan spesifik ditentukan pada bagian project description.

### Requirements

- Melakukan pengambilan data, include orchestration, transformation. i.e., ETL) (Mandatory)
- Melakukan pengambilan data agregasi dari db + excel (Mandatory)
- Melakukan penerapan replication & sharding (Poin plus)
- Mengambil data secara real time (Poin plus)
- Membuat visualisasi (Poin plus)

### Project Description and Expected Delivereble

#### Background

Sebagai langkah strategis menuju efisiensi dan kualitas layanan yang lebih baik, PT. Maju Mundur Asoy memahami betapa pentingnya memantau usia setiap produk yang mereka simpan di gudang mereka. Prinsip ini tidak hanya merupakan langkah proaktif untuk mengelola stok dengan lebih efisien, tetapi juga menjadi landasan untuk kebijakan manajemen inventaris yang cerdas.

Dalam mengelola gudang yang berisikan berbagai produk unggulan, mengetahui usia produk menjadi kunci utama untuk merencanakan penjualan dan promosi dengan lebih tepat sasaran. Dengan memahami siklus hidup setiap item, PT. Maju Mundur Asoy dapat mengidentifikasi produk yang mendekati tanggal kedaluwarsa atau masa paling optimal untuk pemasaran.

Selain itu, pemahaman yang mendalam terhadap usia produk juga membantu perusahaan untuk menjaga kualitas barang yang disimpan. Produk dengan masa simpan yang panjang mungkin memerlukan kondisi penyimpanan khusus, sementara produk yang mendekati batas waktu kadaluwarsa perlu dijual atau didistribusikan secepat mungkin untuk menghindari kerugian.

Manfaat lain dari pemantauan usia produk adalah efisiensi dalam perencanaan produksi. PT. Maju Mundur Asoy dapat mengatur produksi berdasarkan permintaan aktual dan menghindari overproduction yang dapat menyebabkan penumpukan stok yang tidak diinginkan.

#### Expected Deliverable

diharapkan nantinya dari tabel yang tersedia akan dibuat sejumlah model sebagai berikut :

1. Model untuk Menghitung Usia Produk (product_age.sql):
    Fungsi: Menghitung usia produk dalam hari berdasarkan tanggal produksi hingga tanggal saat ini.
    Penggunaan: Model ini dapat membantu Anda memantau usia produk secara real-time dan mengidentifikasi produk yang mendekati atau melewati batas waktu kedaluwarsa.

2. Model untuk Menggabungkan Data Transaksi dan Produk (transaction_product.sql):
    Fungsi: Menggabungkan informasi transaksi dengan data produk untuk analisis lebih lanjut.
    Penggunaan: Dengan model ini, Anda dapat menganalisis performa penjualan produk, melihat tren, dan melakukan pemetaan hubungan antara transaksi dan produk tertentu.

3. Model untuk Menganalisis Stok Tersedia (available_stock.sql):
    Fungsi: Menyediakan data stok produk yang tersedia di berbagai lokasi gudang.
    Penggunaan: Model ini membantu Anda memantau dan menganalisis ketersediaan stok produk di setiap lokasi gudang, memungkinkan perencanaan distribusi dan pengelolaan stok yang lebih efisien.

4. Model untuk Statistik Kategori Produk (category_statistics.sql):
    Fungsi: Menghitung jumlah produk dan rata-rata usia produk untuk setiap kategori produk.
    Penggunaan: Dengan model ini, Anda dapat memahami performa kategori produk tertentu, melihat apakah ada kategori yang cenderung memiliki usia produk yang lebih pendek atau lebih panjang.

5. Model untuk Menganalisis Data Waktu (time_analysis.sql):
    Fungsi: Menyediakan statistik transaksi berdasarkan waktu (misalnya, jumlah transaksi per hari).
    Penggunaan: Model ini memungkinkan Anda untuk melihat tren harian transaksi, mengidentifikasi pola atau periode sibuk, dan merencanakan kegiatan bisnis berdasarkan data waktu.

#### Success Criteria

1. Adanya sistem ELT/ETL untuk menciptakan expected deliverables (mandatory) 
2. setiap kali sistem ELT/ETL berjalan, setiap model sesuai degan kondisi terkini (mandatory)
3. visualisasikan model tersebut kedalam dashboard seperti metabase atau semacamnya (mandatory)
4. perubahan data model terjadi secara real time (optional)

#### Documentation

-

#### Assest
dataset berada pada folder dataset

    untuk model, schema nya :
        1. Model untuk Menghitung Usia Produk (product_age.sql):
            - product_id
            - nama_produk
            - usia_hari
        2. Model untuk Menggabungkan Data Transaksi dan Produk (transaction_product.sql):
            - transaction_id
            - jumlah_pembelian
            - jumlah_penjualan
            - tanggal_transaksi
            - nama_produk
            - tanggal_produksi
        3. Model untuk Menganalisis Stok Tersedia (available_stock.sql):
            - product_id
            - nama_produk
            - jumlah_stok
            - lokasi_gudang
        4. Model untuk Statistik Kategori Produk (category_statistics.sql):
            - nama_kategori
            - jumlah_produk
            - rata_usia_produk
        5. Model untuk Menganalisis Data Waktu (time_analysis.sql):
            - tanggal
            - jumlah_transaksi
    


berikut question yang diperlukan dalam pembuatan dashboard / visualisasi nya

    Pertanyaan Mengenai Produk:
        1. "Apa saja produk-produk dengan usia paling muda dan paling tua?"
        2. "Bagaimana distribusi usia produk di seluruh kategori?"
        3. "Berapa jumlah produk di setiap kategori?"

    Pertanyaan Mengenai Transaksi:
        1. "Bagaimana tren penjualan produk dari waktu ke waktu?"
        2. "Produk apa yang memiliki jumlah pembelian paling tinggi?"
        3. "Berapa jumlah penjualan produk di setiap kategori?"

    Pertanyaan Mengenai Stok:
        1. "Berapa jumlah stok yang tersedia di setiap lokasi gudang?"
        2. "Bagaimana distribusi stok produk di antara lokasi gudang?"
        3. "Apa saja produk yang hampir habis stok?"

    Pertanyaan Mengenai Kategori Produk:
        1. "Berapa rata-rata usia produk di setiap kategori?"
        2. "Apa saja kategori produk yang paling populer berdasarkan jumlah produk?"
        3. "Bagaimana distribusi usia produk di setiap kategori?"

    Pertanyaan Mengenai Waktu:
        1. "Bagaimana tren jumlah transaksi dari waktu ke waktu?"
        2. "Apa saja hari paling sibuk dalam hal transaksi?"
        3. "Bagaimana distribusi jumlah transaksi per hari?"

    Pertanyaan Mengenai Pelanggan dan Pemasok:
        1. "Siapa pelanggan yang paling sering melakukan transaksi?"
        2. "Berapa jumlah transaksi yang dilakukan oleh setiap pelanggan?"
        3. "Apa saja pemasok yang menyediakan produk dengan jumlah stok terbanyak?"

## 📆 Schedule Meeting and Format Mentoring

### Schedule Mentoring

- Mentoring dilakukan 3x dalam sepekan dengan alokasi 60 menit mentoring tiap sesi.
- Jadwal Mentoring dapat menyesuaikan jadwal mentor dan disepakati bersama dengan team, jika ada perubahan mentor dan tim terkait bisa langsung mengkomunikasikan.
- Mentoring bisa dilakukan hari senin-jumat atau sabtu-minggu sesuai availability mentor dan team.

### Mentoring Alocation

| Mentoring | Allocation Time | Agenda                                                      |
| --------- | --------------- | ----------------------------------------------------------- |
| Part 1    | 15 minutes      | Update Team in General                                      |
|           |                 | Update Every Member of The Team                             |
|           |                 | Showing Progress Based On Project Management Tools (Trello) |
| Part 2    | 45 minutes      | Discussion topics according to the problem at hand          |

## ⚠️ General Rules

### Hal-hal yang harus dilakukan oleh Mentees dan Team

- Setiap individu wajib berkontribusi & aktif berkomunikasi dalam team (yang tidak berkontribusi maka tidak mendapatkan nilai, nilai diberikan kenapa yang berkontribusi aktif).
- Setiap team wajib memiliki group komunikasi (membuat group telegram).
- Setiap team wajib menggunakan trello untuk management task & membagi task dg proporsional setiap member.
- Setiap team wajib mengadakan daily meeting setiap hari untuk berkoordinasi.

### Tindakan yang dianggap sebagai pelanggaran bagi Mentees dan Team

- Individu yang tidak aktif atau slow response dalam berkomunikasi dg tim (tidak membalas komunikasi team lebih dari 2 jam saat jam aktif: 9 am - 9 pm).
- Individu tidak ikut berkontribusi dalam mengerjakan task.
- Tim yang tidak membuat group komunikasi.
- Tim tidak menggunakan trello.
- Tim tidak melakukan pembagian tugas.
- Tim yang tidak mengadakan daily meeting.
