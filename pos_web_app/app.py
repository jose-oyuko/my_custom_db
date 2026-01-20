import sys
import os
from flask import Flask, render_template, request, redirect, url_for, session, flash
from datetime import datetime

# Add src to path to use our DB engine
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage import Database
from executor import Executor

app = Flask(__name__)
app.secret_key = 'mydb-pos-secret-key-2026'  # For session management

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
    
    # Get cart from session
    cart = session.get('cart', {})
    
    # Calculate cart total
    cart_total = 0
    cart_items = []
    for product_id, quantity in cart.items():
        # Get product details
        product = executor.execute(f"SELECT * FROM products WHERE id = {product_id}")
        if product:
            p = product[0]
            subtotal = int(p['price']) * quantity
            cart_total += subtotal
            cart_items.append({
                'id': p['id'],
                'name': p['name'],
                'price': p['price'],
                'quantity': quantity,
                'subtotal': subtotal
            })
    
    return render_template('pos.html', 
                          products=products, 
                          cart_items=cart_items,
                          cart_total=cart_total)

@app.route('/add_to_cart/<int:product_id>')
def add_to_cart(product_id):
    """Add item to shopping cart"""
    cart = session.get('cart', {})
    pid = str(product_id)
    
    if pid in cart:
        cart[pid] += 1
    else:
        cart[pid] = 1
    
    session['cart'] = cart
    return redirect(url_for('index'))

@app.route('/remove_from_cart/<int:product_id>')
def remove_from_cart(product_id):
    """Remove item from cart"""
    cart = session.get('cart', {})
    pid = str(product_id)
    
    if pid in cart:
        del cart[pid]
    
    session['cart'] = cart
    return redirect(url_for('index'))

@app.route('/update_cart/<int:product_id>/<int:quantity>')
def update_cart(product_id, quantity):
    """Update item quantity in cart"""
    cart = session.get('cart', {})
    pid = str(product_id)
    
    if quantity > 0:
        cart[pid] = quantity
    else:
        if pid in cart:
            del cart[pid]
    
    session['cart'] = cart
    return redirect(url_for('index'))

@app.route('/checkout', methods=['POST'])
def checkout():
    """Process sale and clear cart"""
    cart = session.get('cart', {})
    
    if not cart:
        return redirect(url_for('index'))
    
    # Get form data
    customer_name = request.form.get('customer_name', 'Guest')
    payment_method = request.form.get('payment_method', 'Cash')
    
    # Calculate total
    total_amount = 0
    sale_items_data = []
    
    for product_id, quantity in cart.items():
        product = executor.execute(f"SELECT * FROM products WHERE id = {product_id}")
        if product:
            p = product[0]
            subtotal = int(p['price']) * quantity
            total_amount += subtotal
            sale_items_data.append({
                'product_id': product_id,
                'quantity': quantity,
                'subtotal': subtotal,
                'stock': p['stock']
            })
    
    # Generate receipt number
    receipt_no = f"RCP{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Get next sale ID
    sales = executor.execute("SELECT id FROM sales")
    sale_id = 1
    if sales:
        sale_id = max(int(s['id']) for s in sales) + 1
    
    # Insert sale record
    sale_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    sql = f"INSERT INTO sales VALUES ({sale_id}, '{receipt_no}', '{customer_name}', '{payment_method}', {total_amount}, '{sale_date}')"
    executor.execute(sql)
    
    # Insert sale items and update stock
    sale_items = executor.execute("SELECT id FROM sale_items")
    item_id = 1
    if sale_items:
        item_id = max(int(i['id']) for i in sale_items) + 1
    
    for item in sale_items_data:
        # Insert sale item
        sql = f"INSERT INTO sale_items VALUES ({item_id}, {sale_id}, {item['product_id']}, {item['quantity']}, {item['subtotal']})"
        executor.execute(sql)
        item_id += 1
        
        # Update product stock
        new_stock = int(item['stock']) - item['quantity']
        sql = f"UPDATE products SET stock = {new_stock} WHERE id = {item['product_id']}"
        executor.execute(sql)
    
    # Clear cart
    session['cart'] = {}
    
    # Show success message
    return render_template('checkout_success.html', 
                          receipt_no=receipt_no, 
                          total=total_amount,
                          customer=customer_name)

@app.route('/products')
def products_page():
    """Product management page"""
    products = executor.execute("SELECT * FROM products")
    return render_template('products.html', products=products)

@app.route('/reports')
def reports_page():
    """Sales reports and analytics"""
    sales = executor.execute("SELECT * FROM sales")
    
    # Calculate today's revenue
    today = datetime.now().strftime("%Y-%m-%d")
    today_sales = [s for s in sales if s['sale_date'].startswith(today)]
    today_revenue = sum(int(s['total_amount']) for s in today_sales)
    
    return render_template('reports.html', 
                          sales=sales,
                          today_revenue=today_revenue,
                          today_count=len(today_sales))

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5001)
