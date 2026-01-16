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
DB_FILE = "pesapal_demo.josedb"
db = Database()
executor = Executor(db, DB_FILE)

# Ensure File Exists and Schema is nice
if os.path.exists(DB_FILE):
    db.load_from_file(DB_FILE)
    print("Loaded existing DB.")
else:
    print("Creating new DB...")
    # Initialize Schema
    executor.execute("CREATE TABLE merchants (id INTEGER PRIMARY KEY, name TEXT UNIQUE, commission INTEGER)")
    executor.execute("CREATE TABLE transactions (id INTEGER PRIMARY KEY, merchant_id INTEGER, amount INTEGER, customer TEXT, status TEXT, date TEXT)")
    
    # Seed Data
    executor.execute("INSERT INTO merchants VALUES (1, 'Java House', 3)")
    executor.execute("INSERT INTO merchants VALUES (2, 'Artcaffe', 5)")
    executor.execute("INSERT INTO transactions VALUES (101, 1, 500, 'John Doe', 'COMPLETED', '2023-10-01')")
    executor.execute("INSERT INTO transactions VALUES (102, 2, 1200, 'Jane Smith', 'COMPLETED', '2023-10-02')")

@app.route('/')
def dashboard():
    # 1. Get Summary Stats
    # Since we don't have COUNT/SUM yet, we fetch all and calculate in Python
    # This is a normal pattern for lighter DBs.
    
    merchants = executor.execute("SELECT * FROM merchants")
    transactions = executor.execute("SELECT * FROM transactions")
    
    total_merchants = len(merchants)
    total_volume = sum(int(t['amount']) for t in transactions)
    total_tx = len(transactions)
    
    # 2. Get Recent Transactions (with JOIN to show Merchant Name!)
    # "SELECT merchants.name, transactions.id, transactions.amount, transactions.customer, transactions.status, transactions.date 
    #  FROM merchants JOIN transactions ON merchants.id = transactions.merchant_id"
    
    sql = "SELECT merchants.name, transactions.id, transactions.amount, transactions.customer, transactions.status, transactions.date FROM merchants JOIN transactions ON merchants.id = transactions.merchant_id"
    recent_tx = executor.execute(sql)
    
    # Sort by date desc (python side) if needed, our DB inserts append so usually sorted by ID
    recent_tx = list(reversed(recent_tx)) # Show new first
    
    return render_template('dashboard.html', 
                           kpis={'merchants': total_merchants, 'volume': total_volume, 'tx': total_tx},
                           transactions=recent_tx)

@app.route('/merchants', methods=['GET', 'POST'])
def merchants_page():
    if request.method == 'POST':
        name = request.form.get('name')
        rate = request.form.get('rate')
        # Simple ID Gen (max + 1)
        current = executor.execute("SELECT id FROM merchants")
        new_id = 1
        if current:
            new_id = max(int(r['id']) for r in current) + 1
            
        sql = f"INSERT INTO merchants VALUES ({new_id}, '{name}', {rate})"
        res = executor.execute(sql)
        if str(res).startswith("Error"):
             return f"Error: {res}"
        return redirect(url_for('merchants_page'))
        
    merchants = executor.execute("SELECT * FROM merchants")
    return render_template('merchants.html', merchants=merchants)

@app.route('/terminal', methods=['GET', 'POST'])
def terminal_page():
    merchants = executor.execute("SELECT * FROM merchants")
    
    if request.method == 'POST':
        merchant_id = request.form.get('merchant_id')
        amount = request.form.get('amount')
        customer = request.form.get('customer')
        
        # ID Gen
        current = executor.execute("SELECT id FROM transactions")
        new_id = 100
        if current:
            new_id = max(int(r['id']) for r in current) + 1
            
        date = datetime.now().strftime("%Y-%m-%d %H:%M")
        
        sql = f"INSERT INTO transactions VALUES ({new_id}, {merchant_id}, {amount}, '{customer}', 'COMPLETED', '{date}')"
        executor.execute(sql)
        return redirect(url_for('dashboard'))
        
    return render_template('terminal.html', merchants=merchants)

@app.route('/delete_transaction/<int:tx_id>', methods=['POST'])
def delete_transaction(tx_id):
    sql = f"DELETE FROM transactions WHERE id = {tx_id}"
    executor.execute(sql)
    return redirect(url_for('dashboard'))

@app.route('/update_merchant/<int:merchant_id>', methods=['POST'])
def update_merchant(merchant_id):
    new_rate = request.form.get('commission')
    sql = f"UPDATE merchants SET commission = {new_rate} WHERE id = {merchant_id}"
    executor.execute(sql)
    return redirect(url_for('merchants_page'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
