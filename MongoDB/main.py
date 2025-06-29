from pymongo import MongoClient
from pprint import pprint
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["alcomarket"]
products = db["products"]

# 2. Очистка коллекции перед загрузкой (для повторного запуска)
products.drop()

# 3. Загрузка данных из файла products.json
with open("products.json", "r") as f:
    data = json.load(f)
    products.insert_many(data)

print("\n📦 Все товары:")
for doc in products.find():
    pprint(doc)

print("\n🍷 Вина с рейтингом сомелье > 4.5:")
for doc in products.find({"type": "wine", "rating.sommelier": {"$gt": 4.5}}):
    pprint(doc)

print("\n📑 Только имя и цена всех товаров:")
for doc in products.find({}, {"name": 1, "price": 1, "_id": 0}):
    pprint(doc)

print("\n🌍 Уникальные страны происхождения:")
pprint(products.distinct("country"))

print("\n📊 Средняя цена пива по странам:")
pipeline = [
    {"$match": {"type": "beer"}},
    {"$group": {"_id": "$country", "avgPrice": {"$avg": "$price"}}},
    {"$sort": {"avgPrice": -1}}
]
for doc in products.aggregate(pipeline):
    pprint(doc)

print("\n🔄 Обновим цену Guinness до 2.99:")
products.update_one({"name": "Guinness Draught"}, {"$set": {"price": 2.99}})
pprint(products.find_one({"name": "Guinness Draught"}))

print("\n➕ Добавим поле stock = 100 ко всем товарам:")
products.update_many({}, {"$set": {"stock": 100}})
pprint(products.find_one())

print("\n❌ Удалим товары, которые недоступны (available: false):")
result = products.delete_many({"available": False})
print(f"Удалено документов: {result.deleted_count}")

print("\n📦 Все товары после удаления:")
for doc in products.find():
    pprint(doc)

print("\n📚 Индексы коллекции:")
pprint(products.index_information())

# Создание индекса
products.create_index("type")

print("\n🧾 Индексы после создания индекса по type:")
pprint(products.index_information())

'''
Далее перенесем его сразу же в контейнер. Сделаем это командой  - 

docker cp products.json mongo_db:/products.json
А далее надо распарсить этот документ, чтобы сделать из него 5 документов, где каждый - это отдельный товар. 

docker exec -it mongo_db mongoimport --db alcomarket --collection products --file /products.json --jsonArray
'''