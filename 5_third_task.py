import pymongo
import msgpack
import json

# Загрузка данных из msgpack файла и добавление их в коллекцию
def load_msgpack(file_path):
    with open(file_path, 'rb') as file:
        data = msgpack.unpack(file, raw=False)
    return data

def insert_to_mongo(file_path, collection):
    data = load_msgpack(file_path)
    if isinstance(data, list):
        collection.insert_many(data)
    elif isinstance(data, dict):
        collection.insert_one(data)

# Удаление по salary < 25 000 || salary > 175 000
def query_1(collection):
    result = collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})
    return {"count": result.deleted_count}

# Увеличение возраста на 1
def query_2(collection):
    result = collection.update_many({}, {"$inc": {"age": 1}})
    return {"count": result.modified_count}

# повышение зарплаты на 5% для профессий
def query_3(collection, professions):
    result = collection.update_many({"job": {"$in": professions}}, {"$mul": {"salary": 1.05}})
    return {"count": result.modified_count}

# повышение зарплаты на 7% для городов
def query_4(collection, cities):
    result = collection.update_many({"city": {"$in": cities}}, {"$mul": {"salary": 1.07}})
    return {"count": result.modified_count}

# Повышение зарплаты на 10% по предикату
def query_5(collection, city, professions, age_range):
    result = collection.update_many({
        "$and": [
            {"city": city},
            {"job": {"$in": professions}},
            {"age": {"$gte": age_range[0], "$lte": age_range[1]}}
        ]
    }, {"$mul": {"salary": 1.10}})
    return {"count": result.modified_count}

# Удаление записей по предикату
def query_6(collection, predicate):
    result = collection.delete_many(predicate)
    return {"deleted_count": result.deleted_count}


def main():
    file_path = "data/task_3_item.msgpack"
    db_name = "homework_db"
    collection_name = "hw_collection"

    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    results = {}

    insert_to_mongo(file_path, collection)

    results = {
        "query_0": "Добавил",
        "query_1": query_1(collection),
        "query_2": query_2(collection),
        "query_3": query_3(collection, ["Водитель", "Врач", "IT-специалист"]),
        "query_4": query_4(collection, ["Астана", "Ла-Корунья"]),
        "query_5": query_5(collection, "Астана", ["IT-специалист", "Бухгалтер"], (30, 40)),
        "query_6": query_6(collection, {"salary": {"$lt": 30000}})
    }

    with open("third_task_results.json", "w", encoding='utf-8') as file:
        json.dump(results, file, indent=4, ensure_ascii=False)


main()
