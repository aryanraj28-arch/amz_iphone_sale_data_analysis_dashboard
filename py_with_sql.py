from pathlib import Path
import pandas as pd
import numpy as np
import mysql.connector

# ----------------------------
project_root = Path(__file__).resolve().parent.parent
candidate_paths = [
    project_root / "data_entry" / "iphone_results (1).csv",
    project_root / "iphone_results (1).csv",
    project_root / "data_entry" / "amazon.csv",
    project_root / "amazon.csv",
]

csv_path = next((p for p in candidate_paths if p.exists()), None)

if csv_path is None:
    raise FileNotFoundError("CSV not found. Tried: " + ", ".join(str(p) for p in candidate_paths))

df = pd.read_csv(csv_path)

print("CSV loaded successfully!\n")
print(df.head())

print("\nColumns found:")
print(df.columns.tolist())

# ----------------------------
# 2. Rename columns to standard names
# ----------------------------
df = df.rename(columns={
    "Description": "description",
    "RAM(Random Processing memory)": "ram",
    "Price": "price",
    "Rating": "rating",
    "ReviewCount": "review_count",
})

# ----------------------------
# 3. Create product_id
# ----------------------------
df["product_id"] = "P" + (df.index + 1).astype(str)

# ----------------------------
# 4. Clean columns
# ----------------------------

# Clean RAM (remove GB if present)
if "ram" in df.columns:
    df["ram"] = df["ram"].astype(str).str.replace("GB", "", regex=False).str.strip()

# Clean Price (remove ₹ and commas)
if "price" in df.columns:
    df["price"] = df["price"].astype(str).str.replace("₹", "", regex=False).str.replace(",", "", regex=False).str.strip()

# Clean Rating
if "rating" in df.columns:
    df["rating"] = df["rating"].astype(str).str.strip()

# Clean ReviewCount
if "review_count" in df.columns:
    df["review_count"] = df["review_count"].astype(str).str.replace(",", "", regex=False).str.strip()

# Convert to numeric
for col in ["ram", "price", "rating", "review_count"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Replace NaN with None
df = df.replace({np.nan: None})

# Keep only required columns
df = df[["product_id", "description", "ram", "price", "rating", "review_count"]]

print("\nCleaned Data Preview:")
print(df.head())

# ----------------------------
# 5. Connect to MySQL
# ----------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="28102003"
)

cursor = conn.cursor()

# Create database
cursor.execute("CREATE DATABASE IF NOT EXISTS amazon_iphone_sale")
cursor.execute("USE amazon_iphone_sale")

# ----------------------------
# 6. Create table
# ----------------------------
cursor.execute("DROP TABLE IF EXISTS products")

create_table_query = """
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    description TEXT,
    ram INT,
    price DECIMAL(12,2),
    rating FLOAT,
    review_count INT
)
"""
cursor.execute(create_table_query)

# ----------------------------
# 7. Insert data
# ----------------------------
insert_query = """
INSERT INTO products (
    product_id, description, ram, price, rating, review_count
)
VALUES (%s, %s, %s, %s, %s, %s)
"""

data = [tuple(row) for row in df.itertuples(index=False, name=None)]

cursor.executemany(insert_query, data)
conn.commit()

print(f"\n{cursor.rowcount} rows inserted successfully into MySQL!")

# ----------------------------
# 8. Close connection
# ----------------------------
cursor.close()
conn.close()

print("Done! iPhone product data stored in MySQL.")