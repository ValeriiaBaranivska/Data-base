from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

# Підключення до MongoDB
uri = "mongodb+srv://baranivskavaleriia:RMIPnOaGRnWJjnu9@cluster0.w5gsc40.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri)
db = client["test-1"]  # ваша база даних
collection_item = db["col-1"]  # колекція товарів
collection_order = db["col-2"]  # колекція замовлень

# Функція для видалення дублікатів у колекції товарів
def remove_duplicate_items():
    seen = set()
    for doc in collection_item.find():
        key = (doc["category"], doc["model"], doc["producer"], doc["price"])  # Унікальний ключ
        if key in seen:
            collection_item.delete_one({"_id": doc["_id"]})  # Видалення дублікату
            print(f"Duplicate item {doc['model']} removed.")
        else:
            seen.add(key)

# Функція для видалення дублікатів у колекції замовлень
def remove_duplicate_orders():
    seen = set()
    for doc in collection_order.find():
        key = doc["order_number"]  # Унікальний ключ — номер замовлення
        if key in seen:
            collection_order.delete_one({"_id": doc["_id"]})  # Видалення дублікату
            print(f"Duplicate order {doc['order_number']} removed.")
        else:
            seen.add(key)

# Функція для вставки унікальних товарів
def insert_items(items):
    for item in items:
        existing_item = collection_item.find_one({
            "category": item["category"],
            "model": item["model"],
            "producer": item["producer"],
            "price": item["price"]
        })
        if not existing_item:
            collection_item.insert_one(item)
            print(f"Item {item['model']} inserted.")
        else:
            print(f"Item {item['model']} already exists.")

# Функція для вставки унікальних замовлень
def insert_orders(orders):
    for order in orders:
        existing_order = collection_order.find_one({"order_number": order["order_number"]})
        if not existing_order:
            collection_order.insert_one(order)
            print(f"Order {order['order_number']} inserted.")
        else:
            print(f"Order {order['order_number']} already exists.")

# Дані товарів
items = [
    {
        "category": "Phone",
        "model": "iPhone 6",
        "producer": "Apple",
        "price": 600
    },
    {
        "category": "TV",
        "model": "Sony I32",
        "producer": "Sony",
        "price": 1200
    },
    {
        "category": "Smart Watch",
        "model": "Apple Watch 5",
        "producer": "Apple",
        "price": 230
    },
    {
        "category": "Phone",
        "model": "iPhone 12",
        "producer": "Apple",
        "price": 800
    },
    {
        "category": "TV",
        "model": "Sony Bravia",
        "producer": "Sony",
        "price": 5000
    },
    {
        "category": "Laptop",
        "model": "Aspire 3",
        "producer": "Acer",
        "price": 2300
    },
    {
        "category": "Laptop",
        "model": "MacBook Air 10",
        "producer": "Apple",
        "price": 6000
    }
]

# Вставка товарів в MongoDB
item_ids = []
for item in items:
    result = collection_item.insert_one(item)
    item_ids.append(result.inserted_id)

# Дані замовлень
orders = [
    {
        "order_number" : 1,
        "date" : datetime(2015,4,14),
        "total_sum" : 1923.4,
        "customer" : {
            "name" : "Andrii",
            "surname" : "Rodinov",
            "phones" : [ 9876543, 1234567],
            "address" : "PTI, Peremohy 37, Kyiv, UA"
        },
        "payment" : {
            "card_owner" : "Andrii Rodionov",
            "cardId" : 12345678
        },
        "items_id" : [item_ids[0], item_ids[1]]
    },
    {
        "order_number" : 2,
        "date" : datetime(2016,2,12),
        "total_sum" : 60000,
        "customer" : {
            "name" : "Valeriia",
            "surname" : "Baranivska",
            "phones" : [ 9876543, 1234567],
            "address" : "Novo Polova 321, Lviv, UA"
        },
        "payment" : {
            "card_owner" : "Valeriia Baranivska",
            "cardId" : 12344378278
        },
        "items_id" : [item_ids[1], item_ids[4]]
    },
    {
        "order_number" : 3,
        "date" : datetime(2018,3,9),
        "total_sum" : 23291,
        "customer" : {
            "name" : "Charli",
            "surname" : "Terak",
            "phones" : [ 7329482, 362749],
            "address" : " Rethord 37 ave, London, UK"
        },
        "payment" : {
            "card_owner" : "Charli West",
            "cardId" : 73284926
        },
        "items_id" : [item_ids[2], item_ids[3]]
    },
    {
        "order_number" : 4,
        "date" : datetime(2015,4,14),
        "total_sum" : 500,
        "customer" : {
            "name" : "Peter",
            "surname" : "Smith",
            "phones" : [ 3047293810, 203886482],
            "address" : "Leet st 21/23, Poland"
        },
        "payment" : {
            "card_owner" : "Peter Smith",
            "cardId" : 90381941
        },
        "items_id" : [item_ids[4], item_ids[4]]
    },
{
        "order_number" : 5,
        "date" : datetime(2015,4,14),
        "total_sum" : 600,
        "customer" : {
            "name" : "Peter",
            "surname" : "Smith",
            "phones" : [ 3047293810, 203886482],
            "address" : "Leet st 21/23, Poland"
        },
        "payment" : {
            "card_owner" : "Peter Smith",
            "cardId" : 90381941
        },
        "items_id" : [item_ids[1], item_ids[0]]
    }
]


# Очищення дублікатів перед вставкою
remove_duplicate_items()
remove_duplicate_orders()

# Вставка даних
insert_items(items)
insert_orders(orders)

print("Дані успішно вставлені в MongoDB.")
