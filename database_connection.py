from sqlalchemy import create_engine
import pandas as pd

# PostgreSQL bağlantı bilgileri
DB_USER = "postgres"
DB_PASSWORD = "student"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "pagila"

# Bağlantı string
conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
#conn_string = "postgresql://{}:{}@{}:{}/{}" \
#                       .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

# SQLAlchemy Engine oluştur
engine = create_engine(conn_string)

# Bağlantıyı test etmek için tablo sayısını çek
query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
tables = pd.read_sql(query, engine)

print("Veritabanındaki Tablolar:")
print(tables)
