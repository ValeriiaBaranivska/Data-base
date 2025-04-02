from neo4j import GraphDatabase, basic_auth

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"

# Налаштування підключення
URI = "bolt://35.172.233.134:7687"
AUTH = ("neo4j", "cards-argument-purchases")

driver = GraphDatabase.driver(URI, auth=basic_auth(*AUTH))

# Функція для виконання довільного Cypher-запиту
def execute_query(cypher_query, parameters=None):
    with driver.session(database="neo4j") as session:
        return session.read_transaction(
            lambda tx: tx.run(cypher_query, **(parameters or {})).data()
        )


# Основна функція для створення вузлів і зв’язків у базі Neo4j
def create_schema():
    with driver.session(database="neo4j") as session:
        create_items(session)
        create_customers(session)
        create_orders(session)
        create_relationships(session)

# Додає товари / клієнтів / замовлення, якщо їх ще немає
def create_items(session):
    items = [
        {"id": 1, "name": "Laptop", "price": 1000},
        {"id": 2, "name": "Smartphone", "price": 500},
        {"id": 3, "name": "TV", "price": 2300}
    ]
    for item in items:
        session.execute_write(
            lambda tx: tx.run(
                "MERGE (i:Items {id: $id}) SET i.name = $name, i.price = $price",
                **item
            )
        )
        print(f"Item {item['name']} added.")

def create_customers(session):
    customers = [
        {"id": 1, "name": "Valeriia"},
        {"id": 2, "name": "Patrick"},
        {"id": 3, "name": "Ivan"}
    ]
    for customer in customers:
        session.execute_write(
            lambda tx: tx.run(
                "MERGE (c:Customer {id: $id}) SET c.name = $name",
                **customer
            )
        )
        print(f"Customer {customer['name']} added.")


def create_orders(session):
    orders = [
        {"id": 1, "date": "2023-11-01"},
        {"id": 2, "date": "2023-10-02"},
        {"id": 3, "date": "2023-10-02"},
        {"id": 4, "date": "2025-12-31"}

    ]
    for order in orders:
        session.execute_write(
            lambda tx: tx.run(
                "MERGE (o:Orders {id: $id}) SET o.date = $date",
                **order
            )
        )
        print(f"Order {order['id']} added.")

# Додаємо звязки між товарами / клієнтами / замовленнями
def create_relationships(session):
    relationships = [
        {"customer_id": 1, "order_id": 1},
        {"customer_id": 2, "order_id": 2},
        {"customer_id": 2, "order_id": 3},
        {"customer_id": 3, "order_id": 4},
        {"customer_id": 4, "order_id": 3}
    ]
    for rel in relationships:
        session.execute_write(
            lambda tx: tx.run(
                "MATCH (c:Customer {id: $customer_id}), (o:Orders {id: $order_id}) MERGE (c)-[:BOUGHT]->(o)",
                **rel
            )
        )
        print(f"Customer {rel['customer_id']} linked to Order {rel['order_id']}.")

    # Додаємо товари в замовлення
    order_items = [
        {"order_id": 1, "item_id": 1},  # Laptop -> Order 1
        {"order_id": 2, "item_id": 2},  # Smartphone -> Order 2
        {"order_id": 3, "item_id": 3},  # TV -> Order 3
        {"order_id": 4, "item_id": 2},  # Smartphone -> Order 4
        {"order_id": 1, "item_id": 3},  # TV -> Order 1
        {"order_id": 2, "item_id": 1}   # Laptop -> Order 2
    ]
    for rel in order_items:
        session.execute_write(
            lambda tx: tx.run(
                "MATCH (o:Orders {id: $order_id}), (i:Items {id: $item_id}) MERGE (o)-[:CONTAINS]->(i)",
                **rel
            )
        )
        print(f"Order {rel['order_id']} contains Item {rel['item_id']}.")

    # Додаємо перегляди товарів клієнтами
    views = [
        {"customer_id": 1, "item_id": 2},  #  на смартфон
        {"customer_id": 2, "item_id": 1},   #  на ноутбук
        {"customer_id": 3, "item_id": 3},  # на телевізор
        {"customer_id": 2, "item_id": 2}  # на смартфон
    ]
    for rel in views:
        session.execute_write(
            lambda tx: tx.run(
                "MATCH (c:Customer {id: $customer_id}), (i:Items {id: $item_id}) MERGE (c)-[:VIEWED]->(i)",
                **rel
            )
        )
        print(f"Customer {rel['customer_id']} viewed Item {rel['item_id']}.")
