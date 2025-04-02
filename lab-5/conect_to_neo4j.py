from neo4j import GraphDatabase, basic_auth

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"

# Налаштування підключення
URI = "bolt://35.172.233.134:7687"
AUTH = ("neo4j", "cards-argument-purchases")

driver = GraphDatabase.driver(URI, auth=basic_auth(*AUTH))


def execute_query(cypher_query, parameters=None):
    """Функція для виконання довільного Cypher-запиту"""
    with driver.session(database="neo4j") as session:
        return session.read_transaction(
            lambda tx: tx.run(cypher_query, **(parameters or {})).data()
        )
def create_schema():
    """Основна функція для створення вузлів і зв’язків у базі Neo4j"""
    with driver.session(database="neo4j") as session:
        create_items(session)
        create_customers(session)
        create_orders(session)
        create_relationships(session)


def create_items(session):
    """Додає товари, якщо їх ще немає"""
    items = [
        {"id": 1, "name": "Laptop", "price": 1000},
        {"id": 2, "name": "Smartphone", "price": 500}
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
    """Додає клієнтів, якщо їх ще немає"""
    customers = [
        {"id": 1, "name": "Valeriia"},
        {"id": 2, "name": "Tom"}
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
    """Додає замовлення, якщо їх ще немає"""
    orders = [
        {"id": 1, "date": "2023-10-01"},
        {"id": 2, "date": "2023-10-02"}
    ]
    for order in orders:
        session.execute_write(
            lambda tx: tx.run(
                "MERGE (o:Orders {id: $id}) SET o.date = $date",
                **order
            )
        )
        print(f"Order {order['id']} added.")


def create_relationships(session):
    """Додає зв’язки між замовленнями, товарами та клієнтами"""
    relationships = [
        {"customer_id": 1, "order_id": 1},
        {"customer_id": 2, "order_id": 2}
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
        {"order_id": 2, "item_id": 2}   # Smartphone -> Order 2
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
        {"customer_id": 1, "item_id": 2},  # Valeriia дивилася на смартфон
        {"customer_id": 2, "item_id": 1}   # Tom дивився на ноутбук
    ]
    for rel in views:
        session.execute_write(
            lambda tx: tx.run(
                "MATCH (c:Customer {id: $customer_id}), (i:Items {id: $item_id}) MERGE (c)-[:VIEWED]->(i)",
                **rel
            )
        )
        print(f"Customer {rel['customer_id']} viewed Item {rel['item_id']}.")
