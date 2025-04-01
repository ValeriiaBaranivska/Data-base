from conect_to_atlas import db
from datetime import datetime

cc_3 = "reviews_capped"
db.create_collection(cc_3, capped=True, size=100000, max=5)

print(f"Capped collection '{cc_3}' cтворено")

def insert_review(collection, review_id, customer_name, review_text, rating):
    review = {
        "review_id": review_id,
        "customer_name": customer_name,
        "review_text": review_text,
        "rating": rating,
        "date": datetime.now()
    }
    collection.insert_one(review)
    print(f"Відгук {review_id} додано")

capped_collection = db[cc_3] # колекція відгуків

#Вставка більше 5 відгуків, для перевірки
for i in range(1, 8):
    insert_review(capped_collection, i, f"Користувач {i}", f"Відгук користувача на товар {i}", i * 1.0)

reviews = capped_collection.find()
print("Reviews in the capped collection:")
for review in reviews:
    print(review)