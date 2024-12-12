from pymongo import MongoClient

# Подключаемся к серверу MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Выбираем базу данных и коллекцию
db = client['homework_db']
collection = db['hw_collection']

db.hw_collection.delete_many({})
documents = collection.find()

for document in documents:
    print(document)