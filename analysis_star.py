import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

# PostgreSQL Bağlantısı
DB_CONFIG = {
    "dbname": "pagila",
    "user": "postgres",
    "password": "student",
    "host": "127.0.0.1",
    "port": "5432"
}

engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

def fetch_data(query):
    """ SQL sorgusunu çalıştırır ve sonucu döndürür. """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# 1️⃣ factSales Tablosunu Doğrudan Sorgula
query_factSales = """
SELECT movie_key, date_key, customer_key, amount
FROM factSales
LIMIT 5;
"""
fact_sales_data = fetch_data(query_factSales)
print("\nFactSales Verileri:")
print(fact_sales_data)

# 2️⃣ JOIN ile Daha Detaylı Analiz Yap
query_joined = """
SELECT dimMovie.title, dimDate.month, dimCustomer.city, SUM(amount) AS revenue
FROM factSales 
JOIN dimMovie    ON (dimMovie.movie_key      = factSales.movie_key)
JOIN dimDate     ON (dimDate.date_key        = factSales.date_key)
JOIN dimCustomer ON (dimCustomer.customer_key = factSales.customer_key)
GROUP BY (dimMovie.title, dimDate.month, dimCustomer.city)
ORDER BY dimMovie.title, dimDate.month, dimCustomer.city, revenue DESC;
"""
detailed_sales_data = fetch_data(query_joined)
print("\nDetaylı Satış Verileri:")
print(detailed_sales_data.head(20))
