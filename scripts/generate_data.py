import pandas as pd
import random
from datetime import datetime, timedelta
import uuid
import os

# Output directory
OUTPUT_DIR = "ecommerce_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
NUM_ORDERS = 3000

random.seed(42)

# -----------------------
# Generate Customers
# -----------------------
customers = []
for i in range(NUM_CUSTOMERS):
    customers.append({
        "customer_id": f"CUST_{i}",
        "name": f"Customer_{i}",
        "email": f"customer{i}@example.com",
        "country": random.choice(["India", "USA", "UK", "Germany"]),
        "signup_date": (datetime.now() - timedelta(days=random.randint(10, 1000))).strftime("%Y-%m-%d")
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)

# -----------------------
# Generate Products
# -----------------------
categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

products = []
for i in range(NUM_PRODUCTS):
    products.append({
        "product_id": f"PROD_{i}",
        "product_name": f"Product_{i}",
        "category": random.choice(categories),
        "price": round(random.uniform(5, 500), 2)
    })

products_df = pd.DataFrame(products)
products_df.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)

# -----------------------
# Generate Orders
# -----------------------
statuses = ["PLACED", "SHIPPED", "CANCELLED"]

orders = []
for i in range(NUM_ORDERS):
    orders.append({
        "order_id": f"ORD_{i}",
        "customer_id": random.choice(customers_df["customer_id"].tolist()),
        "order_date": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d"),
        "status": random.choice(statuses)
    })

orders_df = pd.DataFrame(orders)
orders_df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)

# -----------------------
# Generate Order Items
# -----------------------
order_items = []

for order_id in orders_df["order_id"]:
    for _ in range(random.randint(1, 4)):
        product = products_df.sample(1).iloc[0]
        quantity = random.randint(1, 3)

        order_items.append({
            "order_item_id": str(uuid.uuid4()),
            "order_id": order_id,
            "product_id": product["product_id"],
            "quantity": quantity,
            "price": product["price"]
        })

order_items_df = pd.DataFrame(order_items)
order_items_df.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False)

print("✅ E-commerce data generated successfully in folder:", OUTPUT_DIR)  