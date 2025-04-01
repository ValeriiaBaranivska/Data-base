from datetime import datetime
from bson import ObjectId

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
    "items_id" : [ObjectId("67eba78754efd3600024bf59"), ObjectId("67eba78754efd3600024bf5e")]
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
    "items_id" : [ObjectId("67eba78754efd3600024bf5e"), ObjectId("67eba78754efd3600024bf5b")]
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
    "items_id" : [ObjectId("67eba78754efd3600024bf5d"), ObjectId("67eba78754efd3600024bf5a")]
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
    "items_id" : [ObjectId("67eba78754efd3600024bf5b"), ObjectId("67eba78754efd3600024bf5b")]
    }
]

capped_col = [
{
    "review_id": ObjectId,
    "customer_name": "string",
    "review_text": "string",
    "rating": int,
    "date": datetime
}
]