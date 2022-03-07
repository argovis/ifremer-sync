# usage: python translateProfile.py <absolute paths to all Argo netcdf files relevant for this profile>
# populates mongodb argo/profilesx with the contents of the provided file
# assumes establishCollection.py was run first to create the collection with schema enforcement
# assumes rsync populated content under /ifremer, same as choosefiles.py

import sys, xarray, re, datetime, difflib, pprint, numpy
from pymongo import MongoClient
import util.helpers as h

client = MongoClient('mongodb://database/argo')
db = client.argo

print('parsing', sys.argv[1:])

# look for mandatory and optional keys, complain appropriately

# extract metadata for each file and make sure it's consistent between all files being merged
separate_metadata = [h.extract_metadata(x) for x in sys.argv[1:]]
if not h.compare_metadata(separate_metadata):
	print('error: files', sys.argv[1:], 'did not yield consistent metadata')

# extract data variables for each file separately
separate_data = [h.extract_data(x) for x in sys.argv[1:]]

# merge metadata into single object
metadata = h.merge_metadata(separate_metadata)

# merge data into single pressure axis list
data = h.merge_data(separate_data)

# combine metadata + data and return
profile = {**metadata, **data}
#print(profile)

# write to mongo
try:
	db.profs.insert_one(profile)
except BaseException as err:
	print('error: db write failure')
	print(err)
	print(profile)