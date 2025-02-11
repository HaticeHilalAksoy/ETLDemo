
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

#step 3:
#Films-payment-inventory 
query2="""
SELECT film_id, title, release_year, rental_rate,rating from film limit 5;
select * from payment limit 5;
select * from inventory limit 5;
"""
result1 = pd.read_sql(query2, engine)
print(result1)

#Get the movie of every payment
query3="""
SELECT f.title, p.amount, p.payment_date, p.customer_id                                            
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
limit 5;
"""
result2 = pd.read_sql(query3, engine)
print(result2)
                     
#sum movie rental revenue
query4="""
SELECT f.title, SUM(p.amount) as revenue                                            
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
GROUP BY f.title
order by title, revenue desc
limit 10
"""
result3 = pd.read_sql(query4, engine)
print(result3)

#Get the city of each payment
query5="""
SELECT p.customer_id, p.rental_id, p.amount, ci.city                            
FROM payment p
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
order by p.payment_date
limit 10;
"""
result4 = pd.read_sql(query5, engine)
print(result4)

#Top grossing cities
query6="""
SELECT ci.city as city,SUM(p.amount) as revenue                            
FROM payment p
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
GROUP BY ci.city
order by revenue desc
limit 10

"""
result5 = pd.read_sql(query6, engine)
print(result5)

#Total revenue by month
query7="""
SELECT sum(p.amount) as revenue, EXTRACT(month FROM p.payment_date) as month
from payment p
group by month
order by revenue desc
limit 10;
"""
result6 = pd.read_sql(query7, engine)
print(result6)

#Each movie by customer city and by month (data cube)
query8="""
SELECT f.title, p.amount, p.customer_id, ci.city, p.payment_date,EXTRACT(month FROM p.payment_date) as month
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
order by p.payment_date
limit 10;
"""
result7 = pd.read_sql(query8, engine)
print(result7)

#Sum of revenue of each movie by customer city and by month
query9="""
SELECT f.title, ci.city,EXTRACT(month FROM p.payment_date) as month ,SUM(p.amount) as revenue
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
GROUP BY f.title,ci.city,month
order by month,revenue desc
limit 10;
"""
result8 = pd.read_sql(query9, engine)
print(result8)
