from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from insertion import items, orders


uri = "mongodb+srv://baranivskavaleriia:RMIPnOaGRnWJjnu9@cluster0.w5gsc40.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Вибір бази даних
db = client["test-1"]

# колекція товарів
collection_item = db["col-1"]

collection_item.insert_many(items)
print("Товари успішно додані!")

# колекція замовлень
coll_order = db["col-2"]

coll_order.insert_many(orders)
print("Замовлення успішно додані!")

#print(db.list_collection_names())