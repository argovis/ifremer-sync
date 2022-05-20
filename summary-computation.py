from pymongo import MongoClient
from bson.son import SON
import datetime

client = MongoClient('mongodb://database/argo')
db = client.argo

# what Argo platforms have BGC data? Response to /platforms/bgc
bgcs = [
    {'$match': {'source_info.source':'argo_bgc' }}, 
    {'$project': {'platform_id':1}}, 
    {'$group': {'_id':'$platform_id'}}
]
bgc = db.profiles.aggregate(bgcs)
## write to mongo
try:
    db.summaries.replace_one({"_id": 'argo_bgc'}, {"_id": 'argo_bgc', "summary":{"platforms":list(bgc)}}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(list(bgc))

# summary stats for each data center; response to /dacs
dacsummary = [
    {'$sort': SON([('data_center',1), ('timestamp',-1)])}, 
    {'$group': {'_id': '$data_center','number_of_profiles': {'$sum':1}, 'most_recent_date':{'$first':'$timestamp'}}}  
]
dacs = db.profiles.aggregate(dacsummary)
try:
    db.summaries.replace_one({"_id": 'dacs'}, {"_id": 'dacs', "summary": {"dacs": list(dacs)}}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(list(dacs))

