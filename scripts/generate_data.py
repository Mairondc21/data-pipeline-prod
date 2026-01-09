from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import random
from decimal import Decimal

fake = Faker('pt_BR')
Faker.seed(42)
random.seed(42)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce',
    'user': 'postgres',
    'password': 'postgres'
}

NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 500
NUM_ORDERS = 100000
NUM_REVIEWS = 50000

CATEGORIES = [
    'Eletrônicos', 'Moda', 'Casa e Decoração', 'Esportes',
    'Livros', 'Beleza', 'Brinquedos', 'Alimentos',
    'Ferramentas', 'Automotivo', 'Pet Shop', 'Saúde',
    'Informática', 'Móveis', 'Jardim', 'Bebidas',
    'Papelaria', 'Games', 'Música', 'Cama, Mesa e Banho'
]

def create_tables(conn):
    cursor = conn.cursor()
    
    cursor.execute("""
        DROP TABLE IF EXISTS reviews CASCADE;
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS products CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;
    """)
    
    # Customers
    cursor.execute("""
        CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255) UNIQUE,
            phone VARCHAR(20),
            cpf VARCHAR(14) UNIQUE,
            birth_date DATE,
            gender VARCHAR(20),
            city VARCHAR(100),
            state VARCHAR(2),
            zipcode VARCHAR(10),
            address VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Products
    cursor.execute("""
        CREATE TABLE products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(255),
            category VARCHAR(100),
            price DECIMAL(10, 2),
            cost DECIMAL(10, 2),
            stock_quantity INTEGER,
            supplier VARCHAR(100),
            weight_kg DECIMAL(5, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Orders
    cursor.execute("""
        CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(customer_id),
            order_date TIMESTAMP,
            status VARCHAR(50),
            total_amount DECIMAL(10, 2),
            shipping_cost DECIMAL(10, 2),
            payment_method VARCHAR(50),
            shipping_address VARCHAR(255),
            shipping_city VARCHAR(100),
            shipping_state VARCHAR(2),
            shipping_zipcode VARCHAR(10),
            estimated_delivery DATE,
            actual_delivery DATE
        );
    """)
    
    # Order Items
    cursor.execute("""
        CREATE TABLE order_items (
            order_item_id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES orders(order_id),
            product_id INTEGER REFERENCES products(product_id),
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            discount DECIMAL(10, 2) DEFAULT 0,
            subtotal DECIMAL(10, 2)
        );
    """)
    
    # Reviews
    cursor.execute("""
        CREATE TABLE reviews (
            review_id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(product_id),
            customer_id INTEGER REFERENCES customers(customer_id),
            order_id INTEGER REFERENCES orders(order_id),
            rating INTEGER CHECK (rating BETWEEN 1 AND 5),
            review_title VARCHAR(255),
            review_text TEXT,
            review_date TIMESTAMP,
            helpful_votes INTEGER DEFAULT 0
        );
    """)
    conn.commit()

def generate_customers(conn, num_customers):
    cursor = conn.cursor()
    
    customers = []
    for _ in range(num_customers):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        phone = fake.phone_number()
        cpf = fake.unique.cpf()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)
        gender = random.choice(['Masculino', 'Feminino', 'Outro'])
        city = fake.city()
        state = fake.estado_sigla()
        zipcode = fake.postcode()
        address = fake.street_address()
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        
        customers.append((
            first_name, last_name, email, phone, cpf, birth_date,
            gender, city, state, zipcode, address, created_at
        ))
    
    execute_batch(cursor, """
        INSERT INTO customers (first_name, last_name, email, phone, cpf, birth_date,
                              gender, city, state, zipcode, address, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, customers)
    
    conn.commit()
    print(f"✅ {num_customers} clientes gerados!")

def generate_products(conn, num_products):
    cursor = conn.cursor()
    
    products = []
    for _ in range(num_products):
        product_name = fake.catch_phrase()
        category = random.choice(CATEGORIES)
        price = round(random.uniform(10, 5000), 2)
        cost = round(price * random.uniform(0.3, 0.7), 2)
        stock_quantity = random.randint(0, 1000)
        supplier = fake.company()
        weight_kg = round(random.uniform(0.1, 50), 2)
        created_at = fake.date_time_between(start_date='-2y', end_date='now')
        
        products.append((
            product_name, category, price, cost, stock_quantity,
            supplier, weight_kg, created_at, created_at
        ))
    
    execute_batch(cursor, """
        INSERT INTO products (product_name, category, price, cost, stock_quantity,
                            supplier, weight_kg, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, products)
    
    conn.commit()
    print(f"✅ {num_products} produtos gerados!")

def generate_orders(conn, num_orders):
    """Gera dados de pedidos e itens"""
    cursor = conn.cursor()

    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT product_id, price FROM products")
    products = cursor.fetchall()
    
    orders = []
    order_items = []
    
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    payment_methods = ['Credit Card', 'Debit Card', 'PIX', 'Boleto', 'PayPal']
    
    for i in range(num_orders):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-2y', end_date='now')
        status = random.choice(statuses)
        payment_method = random.choice(payment_methods)
        
        shipping_address = fake.street_address()
        shipping_city = fake.city()
        shipping_state = fake.estado_sigla()
        shipping_zipcode = fake.postcode()
        shipping_cost = round(random.uniform(5, 50), 2)
        
        estimated_delivery = order_date + timedelta(days=random.randint(3, 15))
        actual_delivery = None
        if status == 'Delivered':
            actual_delivery = order_date + timedelta(days=random.randint(3, 12))
        
        num_items = random.randint(1, 5)
        selected_products = random.sample(products, num_items)
        
        total_amount = 0
        for product_id, price in selected_products:
            quantity = random.randint(1, 3)
            unit_price = float(price)
            discount = round(random.uniform(0, unit_price * 0.2), 2)
            subtotal = round((unit_price * quantity) - discount, 2)
            total_amount += subtotal
            
            order_items.append((
                i + 1,
                product_id,
                quantity,
                unit_price,
                discount,
                subtotal
            ))
        
        total_amount = round(total_amount + shipping_cost, 2)
        
        orders.append((
            customer_id, order_date, status, total_amount, shipping_cost,
            payment_method, shipping_address, shipping_city, shipping_state,
            shipping_zipcode, estimated_delivery, actual_delivery
        ))
    
    execute_batch(cursor, """
        INSERT INTO orders (customer_id, order_date, status, total_amount, shipping_cost,
                          payment_method, shipping_address, shipping_city, shipping_state,
                          shipping_zipcode, estimated_delivery, actual_delivery)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, orders)
    
    execute_batch(cursor, """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount, subtotal)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, order_items)
    
    conn.commit()

def generate_reviews(conn, num_reviews):
    """Gera dados de avaliações"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT o.order_id, o.customer_id, oi.product_id, o.order_date
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.status = 'Delivered'
    """)
    
    delivered_orders = cursor.fetchall()
    
    if len(delivered_orders) == 0:
        return
    
    reviews = []
    num_reviews = min(num_reviews, len(delivered_orders))
    
    for _ in range(num_reviews):
        order_id, customer_id, product_id, order_date = random.choice(delivered_orders)
        
        rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 30, 40])[0]
        review_title = fake.sentence(nb_words=6)
        review_text = fake.text(max_nb_chars=500) if rating <= 3 else fake.text(max_nb_chars=300)
        review_date = order_date + timedelta(days=random.randint(1, 30))
        helpful_votes = random.randint(0, 100)
        
        reviews.append((
            product_id, customer_id, order_id, rating,
            review_title, review_text, review_date, helpful_votes
        ))
    
    execute_batch(cursor, """
        INSERT INTO reviews (product_id, customer_id, order_id, rating,
                           review_title, review_text, review_date, helpful_votes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, reviews)
    
    conn.commit()

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_tables(conn)
        
        generate_customers(conn, NUM_CUSTOMERS)
        generate_products(conn, NUM_PRODUCTS)
        generate_orders(conn, NUM_ORDERS)
        generate_reviews(conn, NUM_REVIEWS)
        
        conn.close()
        print("Dados gerados com sucesso!")
        
    except Exception as e:
        print(f"Erro: {e}")
        raise

if __name__ == "__main__":
    main()