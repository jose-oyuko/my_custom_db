"""
Database initialization script for MyDB-POS
Creates tables and seeds sample cafe product data
"""
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage import Database
from executor import Executor

# Initialize database
DB_FILE = "pos_store.josedb"
db = Database()
executor = Executor(db, DB_FILE)

print("Initializing POS Database...")

# Create tables
print("\n1. Creating products table...")
executor.execute("""
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        sku TEXT UNIQUE,
        category TEXT,
        price INTEGER,
        stock INTEGER
    )
""")

print("2. Creating sales table...")
executor.execute("""
    CREATE TABLE sales (
        id INTEGER PRIMARY KEY,
        receipt_no TEXT UNIQUE,
        customer_name TEXT,
        payment_method TEXT,
        total_amount INTEGER,
        sale_date TEXT
    )
""")

print("3. Creating sale_items table...")
executor.execute("""
    CREATE TABLE sale_items (
        id INTEGER PRIMARY KEY,
        sale_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        subtotal INTEGER
    )
""")

# Seed sample products (Cafe theme)
print("\n4. Seeding sample products...")

products = [
    # Hot Beverages
    (1, 'Espresso', 'BEV001', 'Hot Beverages', 150, 50),
    (2, 'Cappuccino', 'BEV002', 'Hot Beverages', 200, 45),
    (3, 'Latte', 'BEV003', 'Hot Beverages', 220, 40),
    (4, 'Americano', 'BEV004', 'Hot Beverages', 180, 35),
    (5, 'Hot Chocolate', 'BEV005', 'Hot Beverages', 250, 30),
    
    # Cold Beverages
    (6, 'Iced Coffee', 'BEV006', 'Cold Beverages', 230, 25),
    (7, 'Iced Tea', 'BEV007', 'Cold Beverages', 180, 20),
    (8, 'Smoothie', 'BEV008', 'Cold Beverages', 300, 15),
    
    # Pastries
    (9, 'Croissant', 'PAS001', 'Pastries', 120, 25),
    (10, 'Muffin', 'PAS002', 'Pastries', 150, 30),
    (11, 'Donut', 'PAS003', 'Pastries', 100, 20),
    (12, 'Bagel', 'PAS004', 'Pastries', 130, 15),
    
    # Snacks
    (13, 'Cookie', 'SNK001', 'Snacks', 80, 40),
    (14, 'Brownie', 'SNK002', 'Snacks', 180, 25),
    (15, 'Sandwich', 'SNK003', 'Snacks', 350, 12),
]

for product in products:
    sql = f"INSERT INTO products VALUES ({product[0]}, '{product[1]}', '{product[2]}', '{product[3]}', {product[4]}, {product[5]})"
    executor.execute(sql)
    print(f"   ✓ Added: {product[1]} (Stock: {product[5]})")

print("\n✅ Database initialization complete!")
print(f"📊 Created 3 tables and seeded {len(products)} products")
print(f"💾 Database saved to: {DB_FILE}")

# Verify
print("\n🔍 Verification - Products in database:")
results = executor.execute("SELECT name, category, price, stock FROM products")
print(f"\nTotal products: {len(results)}")
for r in results[:5]:  # Show first 5
    print(f"  - {r['name']} ({r['category']}): KES {r['price']}, Stock: {r['stock']}")
print(f"  ... and {len(results) - 5} more")
