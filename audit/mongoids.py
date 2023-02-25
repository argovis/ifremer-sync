# usage: python dropprofiles.py
# looks through mongo argo:argo and lists ids

from pymongo import MongoClient

client = MongoClient('mongodb://database/argo')
db = client.argo

mongoprofiles = open("mongoprofiles", "w")
mongoids = [x['_id'] for x in list(db.argo.find({}, {'_id':1}))]
for x in mongoids:
    mongoprofiles.write(x)
    mongoprofiles.write('\n')

