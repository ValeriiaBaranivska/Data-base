from conect_to_atlas import db
from datetime import datetime
from pymongo.errors import CollectionInvalid

cc_3 = "reviews_capped"

try:
    db.create_collection(cc_3, capped=True, size=100000, max=5)
    print(f"Capped collection '{cc_3}' створено.")
except CollectionInvalid:
    print(f"Capped collection '{cc_3}' вже існує.")

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

capped_coll = db[cc_3] # колекція відгуків

