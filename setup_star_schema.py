from sqlalchemy import create_engine, text
import pandas as pd
from database_connection import engine  # PostgreSQL bağlantısını dışarıdan al

def create_dim_tables(engine):
    """Create dimension tables for the Star Schema"""
    queries = [
        text("""
        CREATE TABLE IF NOT EXISTS dimDate (
            date_key INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            year SMALLINT NOT NULL,
            quarter SMALLINT NOT NULL,
            month SMALLINT NOT NULL,
            day SMALLINT NOT NULL,
            week SMALLINT NOT NULL,
            is_weekend BOOLEAN
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS dimCustomer (
            customer_key SERIAL PRIMARY KEY,
            customer_id SMALLINT NOT NULL,
            first_name VARCHAR(45) NOT NULL,
            last_name VARCHAR(45) NOT NULL,
            email VARCHAR(50),
            address VARCHAR(50) NOT NULL,
            address2 VARCHAR(50),
            district VARCHAR(20) NOT NULL,
            city VARCHAR(50) NOT NULL,
            country VARCHAR(50) NOT NULL,
            postal_code VARCHAR(10),
            phone VARCHAR(20) NOT NULL,
            active SMALLINT NOT NULL,
            create_date TIMESTAMP NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS dimMovie (
            movie_key SERIAL PRIMARY KEY,
            film_id SMALLINT NOT NULL,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            release_year SMALLINT,
            language VARCHAR(20) NOT NULL,
            original_language VARCHAR(20),
            rental_duration SMALLINT NOT NULL,
            length SMALLINT NOT NULL,
            rating VARCHAR(5) NOT NULL,
            special_features VARCHAR(60) NOT NULL
        );
        """),
        text("""
        CREATE TABLE IF NOT EXISTS dimStore (
            store_key SERIAL PRIMARY KEY,
            store_id SMALLINT NOT NULL,
            address VARCHAR(50) NOT NULL,
            address2 VARCHAR(50),
            district VARCHAR(20) NOT NULL,
            city VARCHAR(50) NOT NULL,
            country VARCHAR(50) NOT NULL,
            postal_code VARCHAR(10),
            manager_first_name VARCHAR(45) NOT NULL,
            manager_last_name VARCHAR(45) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL
        );
        """)
    ]

    with engine.connect() as conn:
        for query in queries:
            conn.execute(query)
        conn.commit()  # Değişiklikleri kaydet
    print("Dimension tables created successfully.")

def create_fact_table(engine):
    """Create fact table for sales"""
    query = text("""
    CREATE TABLE IF NOT EXISTS factSales (
        sales_key SERIAL PRIMARY KEY,
        date_key INTEGER REFERENCES dimDate (date_key),
        customer_key INTEGER REFERENCES dimCustomer (customer_key),
        movie_key INTEGER REFERENCES dimMovie (movie_key),
        store_key INTEGER REFERENCES dimStore (store_key),
        sales_amount NUMERIC
    );
    """)

    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()  # Değişiklikleri kaydet
    print("Fact table created successfully.")

if __name__ == "__main__":
    create_dim_tables(engine)
    create_fact_table(engine)

    # Check the existing tables
    query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name   = 'dimdate';"
    query2="SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'factsales';"
    tables = pd.read_sql(query, engine)
    tables1 = pd.read_sql(query2, engine)
    print("Veritabanındaki Tablolar:")
    print(tables)
    print(tables1)
