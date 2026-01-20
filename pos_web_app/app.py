import sys
import os
from flask import Flask, render_template, request, redirect, url_for
from datetime import datetime

# Add src to path to use our DB engine
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage import Database
from executor import Executor

app = Flask(__name__)

# Initialize DB
DB_FILE = "pos_store.josedb"
db = Database()
executor = Executor(db, DB_FILE)

# Load existing database
if os.path.exists(DB_FILE):
    db.load_from_file(DB_FILE)
    print(f"Loaded existing POS database: {DB_FILE}")
else:
    print(f"Warning: Database file '{DB_FILE}' not found!")
    print("Please run: python pos_web_app/init_db.py")

@app.route('/')
def index():
    """Main POS/Cashier interface"""
    products = executor.execute("SELECT * FROM products")
    return render_template('pos.html', products=products)

@app.route('/products')
def products_page():
    """Product management page"""
    products = executor.execute("SELECT * FROM products")
    return render_template('products.html', products=products)

@app.route('/reports')
def reports_page():
    """Sales reports and analytics"""
    sales = executor.execute("SELECT * FROM sales")
    return render_template('reports.html', sales=sales)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
