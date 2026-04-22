import sqlite3
import pandas as pd

conn = sqlite3.connect("Project/fuel.db")

df = pd.read_sql("SELECT * FROM fuel_prices LIMIT 50", conn)

print(df)

conn.close()