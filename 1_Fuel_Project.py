import pandas as pd
import sqlite3
#LOAD DATA
df = pd.read_csv("Project/fuel_price_history_45d.csv")
df.columns = ["date", "city", "fuel_type", "price"]
#VALIDATE DATA
df['date'] = pd.to_datetime(df['date'], errors='coerce')
# Check missing before removing
missing = df[df.isnull().any(axis=1)]
if not missing.empty:
    print("⚠ Missing data found:")
    print(missing)
#handle missing
df = df.ffill()
#Remove invalid rows if still any
df = df.dropna()
#PROCESS DATA
df = df.sort_values(['city', 'fuel_type', 'date'])
#Previous day price
df['prev_price'] = df.groupby(['city','fuel_type'])['price'].shift(1)
#Change detection
def check(row):
    if pd.isna(row['prev_price']):
        return "No Data"
    elif row['price'] > row['prev_price']:
        return "Increase"
    elif row['price'] < row['prev_price']:
        return "Decrease"
    else:
        return "No Change"

df['change'] = df.apply(check, axis=1)
#TREND ANALYSIS
df['7_day_avg'] = df.groupby(['city','fuel_type'])['price'].transform(lambda x: x.rolling(7).mean())
df['30_day_avg'] = df.groupby(['city','fuel_type'])['price'].transform(lambda x: x.rolling(30).mean())

#Highest increase city for last 7 days
latest = df['date'].max()
week = df[df['date'] >= latest - pd.Timedelta(days=7)]
inc = week.groupby('city')['price'].apply(lambda x: x.max() - x.min())
print("Highest Increase City:", inc.idxmax())

#STORE IN SQLITE
conn = sqlite3.connect("fuel.db")
df.to_sql("fuel_prices", conn, if_exists="replace", index=False)
conn.close()
#REPORT
today = df[df['date'] == latest]
print("\nDAILY REPORT")
print("Date:", latest)
for _, r in today.iterrows():
    print(f"{r['city']} ({r['fuel_type']}): {r['price']} → {r['change']}")
print("\nWEEKLY SUMMARY")
print(week.groupby(['city','fuel_type'])['price'].mean())
