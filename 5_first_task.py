import pickle
import pymongo
from bson.json_util import dumps
import json
import random


def load_data(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data


def insert_to_mongo(data, db_name, collection_name):
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    return collection


# Queries
def query_1(collection):
    result = collection.find().sort('salary', pymongo.DESCENDING).limit(10)
    return dumps(result, indent=4)


def query_2(collection):
    result = collection.find({'age': {'$lt': 30}}).sort('salary', pymongo.DESCENDING).limit(15)
    return dumps(result, indent=4)


def query_3(collection, city, job):
    result = collection.find({
        'city': city,
        'job': {'$in': job}
    }).sort('age', pymongo.ASCENDING).limit(10)
    return dumps(result, indent=4)


def query_4(collection, age_range, year_range, salary_ranges):
    result = collection.count_documents({
        'age': {'$gte': age_range[0], '$lte': age_range[1]},
        'year': {'$gte': year_range[0], '$lte': year_range[1]},
        '$or': [
            {'salary': {'$gt': salary_ranges[0][0], '$lte': salary_ranges[0][1]}},
            {'salary': {'$gt': salary_ranges[1][0], '$lte': salary_ranges[1][1]}}
        ]
    })
    return result


def __main__():
    file_path = 'data/task_1_item.pkl'
    db_name = 'homework_db'
    collection_name = 'hw_collection'
    data = load_data(file_path)
    collection = insert_to_mongo(data, db_name, collection_name)

#Попытка, рандома - удачная, но излишняя
    #cities = collection.distinct("city")
    #jobs = collection.distinct("job")
    #city = random.choice(cities)
    #random_jobs = random.sample(jobs, min(3, len(jobs)))

    results = {
        "query_1": json.loads(query_1(collection)),
        "query_2": json.loads(query_2(collection)),
        "query_3": json.loads(query_3(collection, 'Монсон', ['Учитель', 'Архитетор', 'Менеджер'])),
        "query_4": query_4(collection, (25, 40), (2019, 2022), [(50000, 75000), (125000, 150000)])
    }

    with open("first_task_results.json", "w", encoding='utf-8') as file:
        json.dump(results, file, indent=4, ensure_ascii=False)



#print(load_data('data/task_1_item.pkl'))
__main__()

