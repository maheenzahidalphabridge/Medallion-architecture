import sqlite3, random
from datetime import datetime, timedelta
DB_FILE="ecommerce.db"; NUM_CUSTOMERS=100; NUM_ORDERS=500; SEED=42
random.seed(SEED)
conn=sqlite3.connect(DB_FILE); conn.execute("PRAGMA foreign_keys=ON"); cur=conn.cursor()
cur.executescript("""
DROP TABLE IF EXISTS order_items; DROP TABLE IF EXISTS orders; DROP TABLE IF EXISTS products; DROP TABLE IF EXISTS customers;
CREATE TABLE customers(customer_id INTEGER PRIMARY KEY AUTOINCREMENT,first_name TEXT NOT NULL,last_name TEXT NOT NULL,email TEXT UNIQUE NOT NULL,city TEXT,state TEXT,created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE products(product_id INTEGER PRIMARY KEY AUTOINCREMENT,product_name TEXT NOT NULL,category TEXT,price DECIMAL(10,2) NOT NULL,stock_quantity INTEGER NOT NULL DEFAULT 0,created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE orders(order_id INTEGER PRIMARY KEY AUTOINCREMENT,customer_id INTEGER NOT NULL,order_date DATETIME NOT NULL,status TEXT NOT NULL,payment_method TEXT NOT NULL,total_amount DECIMAL(10,2) NOT NULL DEFAULT 0,FOREIGN KEY(customer_id) REFERENCES customers(customer_id));
CREATE TABLE order_items(order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,order_id INTEGER NOT NULL,product_id INTEGER NOT NULL,quantity INTEGER NOT NULL,unit_price DECIMAL(10,2) NOT NULL,FOREIGN KEY(order_id) REFERENCES orders(order_id),FOREIGN KEY(product_id) REFERENCES products(product_id));
""")
first=["John","Sarah","Michael","Emma","David","Sophia","James","Olivia","Daniel","Emily"]; last=["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Taylor"]
loc=[("Seattle","WA"),("Austin","TX"),("New York","NY"),("Chicago","IL"),("Boston","MA"),("San Francisco","CA"),("Denver","CO"),("Miami","FL"),("Atlanta","GA"),("Portland","OR")]
for i in range(NUM_CUSTOMERS):
    a,b=random.choice(first),random.choice(last); city,state=random.choice(loc); created=datetime.now()-timedelta(days=random.randint(30,900))
    cur.execute("INSERT INTO customers(first_name,last_name,email,city,state,created_at) VALUES(?,?,?,?,?,?)",(a,b,f"{a.lower()}.{b.lower()}{i}@example.com",city,state,created.strftime("%Y-%m-%d %H:%M:%S")))
products=[("Laptop","Electronics",999.99),("Wireless Mouse","Electronics",29.99),("Mechanical Keyboard","Electronics",89.99),("Monitor","Electronics",299.99),("Headphones","Electronics",149.99),("Office Chair","Furniture",249.99),("Standing Desk","Furniture",499.99),("Desk Lamp","Furniture",49.99),("Python Programming","Books",44.99),("SQL Fundamentals","Books",39.99),("Data Engineering","Books",54.99),("Backpack","Accessories",69.99),("Water Bottle","Accessories",24.99),("Laptop Stand","Accessories",59.99)]
for n,c,p in products: cur.execute("INSERT INTO products(product_name,category,price,stock_quantity) VALUES(?,?,?,?)",(n,c,p,random.randint(20,250)))
pdata=cur.execute("SELECT product_id,price FROM products").fetchall(); statuses=["Pending","Processing","Shipped","Delivered","Cancelled"]; pays=["Credit Card","Debit Card","PayPal","Apple Pay"]
for _ in range(NUM_ORDERS):
    cid=random.randint(1,NUM_CUSTOMERS); dt=datetime.now()-timedelta(days=random.randint(0,730),hours=random.randint(0,23))
    cur.execute("INSERT INTO orders(customer_id,order_date,status,payment_method,total_amount) VALUES(?,?,?,?,0)",(cid,dt.strftime("%Y-%m-%d %H:%M:%S"),random.choice(statuses),random.choice(pays))); oid=cur.lastrowid; total=0
    for pid,price in random.sample(pdata,random.randint(1,5)):
        qty=random.randint(1,4); total+=float(price)*qty; cur.execute("INSERT INTO order_items(order_id,product_id,quantity,unit_price) VALUES(?,?,?,?)",(oid,pid,qty,price))
    cur.execute("UPDATE orders SET total_amount=? WHERE order_id=?",(round(total,2),oid))
conn.commit()
for t in ["customers","products","orders","order_items"]: print(f"{t}: {cur.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:,} rows")
print(f"Created {DB_FILE}"); conn.close()
