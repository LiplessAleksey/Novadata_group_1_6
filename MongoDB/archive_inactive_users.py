from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

# Конфигурация
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "my_database"
COLLECTION_NAME = "user_events"
ARCHIVE_COLLECTION_NAME = "archived_users"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE_DIR, "MongoDB", "reports")

# Создание папки для отчётов, если нет
if not os.path.exists(REPORTS_DIR):
    os.makedirs(REPORTS_DIR)

# Подключение к MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
archive_collection = db[ARCHIVE_COLLECTION_NAME]

# Даты
current_date = datetime.now()
registration_cutoff = current_date - timedelta(days=30)
activity_cutoff = current_date - timedelta(days=14)

# Находим пользователей с активностью за последние 14 дней
recent_user_ids = collection.distinct(
    "user_id",
    {"event_time": {"$gte": activity_cutoff}}
)

# Находим неактивных пользователей
pipeline = [
    {
        "$match": {
            "user_info.registration_date": {"$lt": registration_cutoff},
            "user_id": {"$nin": recent_user_ids}
        }
    },
    {
        "$group": {
            "_id": "$user_id",
            "email": {"$first": "$user_info.email"},
            "docs": {"$push": "$$ROOT"}
        }
    }
]

inactive_users = list(collection.aggregate(pipeline))

# Архивируем пользователей
archived_count = 0
archived_ids = []

for user in inactive_users:
    # Добавляем документы в архивную коллекцию
    archive_collection.insert_many(user["docs"])

    # Удаляем из основной коллекции
    user_ids = [doc["_id"] for doc in user["docs"]]
    collection.delete_many({"_id": {"$in": user_ids}})

    archived_count += 1
    archived_ids.append(user["_id"])

# Формируем отчет
report = {
    "date": current_date.strftime("%Y-%m-%d"),
    "archived_users_count": archived_count,
    "archived_users_ids": archived_ids
}

# Сохраняем отчет
report_file = os.path.join(REPORTS_DIR, f"{current_date.strftime('%Y-%m-%d')}.json")
with open(report_file, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=4)

print(f"✅ Архивация завершена. Пользователи архивированы: {archived_count}. Отчет: {report_file}")