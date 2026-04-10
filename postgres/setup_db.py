from sqlalchemy import create_engine
from postgres.models import Base

DB_HOST = "itam-northwind-110276528929.cch6quk8gwt3.us-east-1.rds.amazonaws.com"

DB_USER = "itam"
DB_PASSWORD = "Itam1234!"
DB_NAME = "flights"
DB_PORT = 5432

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

if __name__ == "__main__":
    print("Creating tables in PostgreSQL...")
    Base.metadata.create_all(engine)
    print("Tables created successfully")