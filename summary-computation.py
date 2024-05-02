from pymongo import MongoClient
from bson.son import SON
import datetime

client = MongoClient('mongodb://database/argo')
db = client.argo

# dac summary, response to /argo/dacs
dacs = [
    {
       "$lookup":
         {
           "from": "argo",
           "localField": "_id",
           "foreignField": "metadata",
           "pipeline": [
            {"$project": { "timestamp": 1 }},
            {"$sort": {"timestamp":-1}}
           ],
           "as": "data"
         }
    },
    {
        "$project":{
            "data_center": "$data_center",
            "n": {"$size": "$data"},
            "mostrecent": {"$first": "$data.timestamp"}
        }
    },
    {
        "$group":{
            "_id": "$data_center",
            "n": {"$sum": "$n"},
            "mostrecent": {"$max": "$mostrecent"}
        }
    }
]
dacs = list(db.argoMeta.aggregate(dacs))
try:
    db.summaries.replace_one({"_id": 'argo_dacs'}, {"_id": 'argo_dacs', "summary":dacs}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(dacs)

# bgc summary, response to /argo/bgc
bgc = [
    {
       "$lookup":
         {
           "from": "argo",
           "localField": "_id",
           "foreignField": "metadata",
           "pipeline": [
            {"$match": {"source.source":"argo_bgc"}},
            {"$project": { "timestamp": 1 }},
            {"$sort": {"timestamp":-1}}
           ],
           "as": "data"
         }
    },
    {
        "$project":{
            "platform": "$platform",
            "n": {"$size": "$data"},
            "mostrecent": {"$first": "$data.timestamp"}
        }
    },
    {
        "$group":{
            "_id": "$platform",
            "n": {"$sum": "$n"},
            "mostrecent": {"$max": "$mostrecent"}
        }
    },
    {"$match": {"n":{"$gt":0}}}
]
bgc = list(db.argoMeta.aggregate(bgc))
try:
    db.summaries.replace_one({"_id": 'argo_bgc'}, {"_id": 'argo_bgc', "summary":bgc}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(bgc)

# data_keys enumerations
data_keys = list(db['argo'].distinct('data_info.0'))
data_keys.sort()
try:
    db.summaries.replace_one({"_id": 'argo_data_keys'}, {"_id":'argo_data_keys', "data_keys":data_keys}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(data_keys)

# /argo/overview
argo_overview = {
    "nCore": db.argo.count_documents({"source.source": "argo_core"}),
    "nBGC": db.argo.count_documents({"source.source": "argo_bgc"}),
    "nDeep": db.argo.count_documents({"source.source": "argo_deep"}),
    "mostrecent": list(db.argo.aggregate([{"$sort":{"timestamp":-1}},{"$limit":1}]))[0]['timestamp'],
    "latest_argovis_update": datetime.datetime.now(),
    "datacenters": [x['_id'] for x in dacs]
}

try:
    db.summaries.replace_one({"_id": 'argo_overview'}, {"_id": 'argo_overview', "summary":argo_overview}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(argo_overview)

# ----- index preheats ------------

poly = {"$geoWithin": {"$geometry": {"type": "Polygon","coordinates": [[[-135,40],[-135,45],[-130,45],[-130,40],[-135,40]]]}}}
time = {"$gte": datetime.datetime.strptime('2018-11-06T00:00:00Z', "%Y-%m-%dT%H:%M:%SZ"), "$lt": datetime.datetime.strptime('2018-11-07T00:00:00Z', "%Y-%m-%dT%H:%M:%SZ")}

preheat = list(db.argo.aggregate([{'$match': {'geolocation': poly}}])) # argo geolocation index
preheat = list(db.argo.aggregate([{'$match': {'timestamp': time}}]))   # argo timestamp index
preheat = list(db.argo.aggregate([{'$match': {'geolocation': poly, 'timestamp':time}}])) # argo timestamp x geolocation index, as optimized by .explain()
preheat = list(db.cchdo.aggregate([{'$match': {'geolocation': poly}}])) # similar for cchdo
preheat = list(db.cchdo.aggregate([{'$match': {'timestamp': time}}]))
preheat = list(db.cchdo.aggregate([{'$match': {'geolocation': poly, 'timestamp':time}}]))



