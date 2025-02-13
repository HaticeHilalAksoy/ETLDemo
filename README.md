# ETLDemo

1.Gerekli Araçları Yükleme
2.PostgreSQL Veritabanını Kurma
3.VS Code ile PostgreSQL Bağlantısı Kurma
4.Pagila Veritabanını Yükleme
5.VS Code Üzerinden SQL Komutlarını Çalıştırma
6.Veriyi İnceleme ve Analiz Etme
7.ETL Sürecini Gerçekleştirme (Star Schema Dönüşümü)



*Extract (Veriyi Çekme) → PostgreSQL’den veriyi alacağız.
*Transform (Veriyi Dönüştürme) → 3NF'den Star Schema'ya geçiş yapacağız.
*Load (Veriyi Yükleme) → Yeni veri modelimizi oluşturacağız.

-psql -U postgres -d pagila
-PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
-PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
-PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql
-psql -h 127.0.0.1 -U student -d pagila

psql -U postgres --gir
\l --listele
\c pagila --pagila db sine gir
\dt  -- Veritabanındaki tabloları listele


psql -U postgres -d pagila  ---en baştan girdik

#ALTER TABLE dimMovie ADD COLUMN rental_rate NUMERIC(5,2); --eksik tablo elemanlarını ekledim!!!
#ALTER TABLE dimStore ADD COLUMN manager_staff INTEGER;
#ALTER TABLE factSales RENAME COLUMN sales_amount TO amount;



