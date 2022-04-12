# usage: see process-rsync-result.sh
import sys
import util.helpers as h
from pymongo import MongoClient

client = MongoClient('mongodb://database/argo')
db = client.argo

profupdates = open("/tmp/profileUpdates.txt", "w")

with open(sys.argv[1], 'r') as filelist:
	f = filelist.readline()
	while f:
		tokens = f.split('/')
		folder = '/' + '/'.join(tokens[1:5])
		prof_number = h.pickprof(f)
		files = h.select_files(folder, prof_number)
		if len(files)>0:
			for f in files:
				profupdates.write(f + ' ')
			profupdates.write('\n')
		else:
			print('to be deleted: _id', tokens[3]+'_'+prof_number)
			try:
				db.profiles.delete_one({"_id": tokens[3]+'_'+prof_number})
			except BaseException as err:
				print('error: failed to delete', tokens[3]+'_'+prof_number)
				print(err)

		f = filelist.readline()
