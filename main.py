import psycopg2
from clickhouse_driver import Client
from pymongo import MongoClient

# PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="user",
    password="password",
    dbname="example_db"
)
pg_cursor = pg_conn.cursor()
pg_cursor.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT);")
pg_cursor.execute("INSERT INTO users(name) VALUES (%s)", ("Alice",))
pg_cursor.execute("SELECT * FROM users;")
print("PostgreSQL:", pg_cursor.fetchall())
pg_conn.commit()
pg_cursor.close()
pg_conn.close()

# ClickHouse
ch_client = Client(
    host='localhost',
    user='default',
    password='mypassword'
)
ch_client.execute("CREATE TABLE IF NOT EXISTS test (id UInt32, name String)"
                  "ENGINE = MergeTree() ORDER BY id")
ch_client.execute("INSERT INTO test (id, name) VALUES", [(1, 'Bob')])
rows = ch_client.execute("SELECT * FROM test")
print("ClickHouse:", rows)

# MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["test_db"]
mongo_collection = mongo_db["users"]
mongo_collection.insert_one({"name": "Charlie"})
print("MongoDB:", list(mongo_collection.find({}, {"_id": 0})))