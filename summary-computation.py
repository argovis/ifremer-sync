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
def enumerate_data_keys(collection):
    data_keys = []
    if collection!='grid':
        # all data keys are in gridMeta, and there is no actual collection called 'grid'
        data_keys = list(db[collection].distinct('data_keys'))
    data_keys_meta = list(db[collection+'Meta'].distinct('data_keys'))
    data_keys = list(set(data_keys + data_keys_meta))
    data_keys.sort()
    try:
        db.summaries.replace_one({"_id": collection+'_data_keys'}, {"_id": collection+'_data_keys', "data_keys":data_keys}, upsert=True)
    except BaseException as err:
        print('error: db write failure')
        print(err)
        print(data_keys)

enumerate_data_keys('argo')
#enumerate_data_keys('cchdo')
#enumerate_data_keys('drifter')
#enumerate_data_keys('tc')
#enumerate_data_keys('grid')

# /argo/overview
argo_overview = {
    "nCore": db.argo.count_documents({"source.source": "argo_core"}),
    "nBGC": db.argo.count_documents({"source.source": "argo_bgc"}),
    "nDeep": db.argo.count_documents({"source.source": "argo_deep"}),
    "mostrecent": list(db.argo.aggregate([{"$sort":{"timestamp":-1}},{"$limit":1}]))[0]['timestamp'],
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
time = {"$gte": ISODate('2018-11-06T00:00:00Z'), "$lt": ISODate('2018-11-07T00:00:00Z')}

preheat = list(db.argo.aggregate([{'$match': {geolocation: poly}}])) # argo geolocation index
preheat = list(db.argo.aggregate([{'$match': {timestamp: time}}]))   # argo timestamp index
preheat = list(db.argo.aggregate([{'$match': {geolocation: poly, timestamp:time}}])) # argo timestamp x geolocation index, as optimized by .explain()



