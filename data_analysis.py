
#How much? What data sizes are we looking at?
from sqlalchemy import create_engine
import pandas as pd

# PostgreSQL bağlantısını tekrar oluştur
DB_USER = "postgres"
DB_PASSWORD = "student"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "pagila"

conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

# Kaç kayıt olduğunu kontrol etmek için SQL sorguları
queries = {
    "nStores": "SELECT COUNT(*) FROM store;",
    "nFilms": "SELECT COUNT(*) FROM film;",
    "nCustomers": "SELECT COUNT(*) FROM customer;",
    "nRentals": "SELECT COUNT(*) FROM rental;",
    "nPayment": "SELECT COUNT(*) FROM payment;",
    "nStaff": "SELECT COUNT(*) FROM staff;",
    "nCity": "SELECT COUNT(*) FROM city;",
    "nCountry": "SELECT COUNT(*) FROM country;"
}

# Her tablo için veriyi çek ve yazdır
for name, query in queries.items():
    result = pd.read_sql(query, engine)
    print(f"{name} = {result.iloc[0, 0]}")
#When? What time period are we talking about?
query = "SELECT MIN(payment_date) AS start_date, MAX(payment_date) AS end_date FROM payment;"
result = pd.read_sql(query, engine)

print(f"Veritabanındaki Ödeme Tarih Aralığı:\nBaşlangıç: {result.iloc[0, 0]}\nBitiş: {result.iloc[0, 1]}")

#Where? Where do events in this database occur?
query = """
SELECT district, COUNT(*) AS n 
FROM address 
GROUP BY district 
ORDER BY n DESC 
LIMIT 10;
"""

result = pd.read_sql(query, engine)
print("En Fazla Adres Kayıtları Olan Bölgeler:")
print(result)
