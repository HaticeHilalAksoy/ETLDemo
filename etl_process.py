import psycopg2
from datetime import datetime

# PostgreSQL bağlantı bilgileri
DB_CONFIG = {
    "dbname": "pagila",
    "user": "postgres",
    "password": "student",
    "host": "127.0.0.1",
    "port": "5432"
}

def execute_query(query, params=None):
    """ PostgreSQL'de bir SQL sorgusu çalıştırır. """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        cur.close()
        conn.close()
        print("Query executed successfully.")
    except Exception as e:
        print(f"Error: {e}")

def load_dim_customer():
    query = """
    INSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address, 
                             address2, district, city, country, postal_code, phone, active, 
                             create_date, start_date, end_date)
    SELECT 
           c.customer_id AS customer_key,
           c.customer_id,
           c.first_name,
           c.last_name,
           c.email,
           a.address,
           a.address2,
           a.district,
           ci.city,
           co.country,
           a.postal_code,
           a.phone,
           c.active,
           c.create_date,
           NOW()         AS start_date,
           NOW()         AS end_date
    FROM customer c
    JOIN address a  ON c.address_id = a.address_id
    JOIN city ci    ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id;
    """
    execute_query(query)

def load_dim_movie():
    query = """
    INSERT INTO dimMovie (movie_key, title, release_year, language, original_language, rental_duration, rental_rate, length, rating)
    SELECT 
           f.film_id AS movie_key,
           f.title,
           f.release_year,
           l.name AS language,
           orig_lang.name AS original_language,
           f.rental_duration,
           f.rental_rate,
           f.length,
           f.rating
    FROM film f
    JOIN language l              ON f.language_id = l.language_id
    LEFT JOIN language orig_lang ON f.original_language_id = orig_lang.language_id;
    """
    execute_query(query)

def load_dim_store():
    query = """
    INSERT INTO dimStore (store_key, store_id, manager_staff, address, district, city, country, postal_code)
    SELECT 
           s.store_id AS store_key,
           s.store_id,
           s.manager_staff_id,
           a.address,
           a.district,
           ci.city,
           co.country,
           a.postal_code
    FROM store s
    JOIN address a  ON s.address_id = a.address_id
    JOIN city ci    ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id;
    """
    execute_query(query)

def load_fact_sales():
    query = """
    INSERT INTO factSales (sales_key, date_key, customer_key, movie_key, store_key, amount)
    SELECT 
           p.payment_id AS sales_key,
           TO_CHAR(p.payment_date :: DATE, 'yyyyMMDD')::integer AS date_key,
           p.customer_id AS customer_key,
           i.film_id AS movie_key,
           r.inventory_id AS store_key,
           p.amount
    FROM payment p
    JOIN rental r       ON p.rental_id = r.rental_id
    JOIN inventory i    ON r.inventory_id = i.inventory_id;
    """
    execute_query(query)

def main():
    print("Starting ETL process...")
    load_dim_customer()
    load_dim_movie()
    load_dim_store()
    load_fact_sales()
    print("ETL process completed!")

if __name__ == "__main__":
    main()
