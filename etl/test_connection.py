from sqlalchemy import create_engine
engine = create_engine("postgresql://postgres:postgres@localhost:5432/macro_dw")
connection = engine.connect()

print("Database connected successfully!")

connection.close()