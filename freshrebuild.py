# usage: see freshrebuild.sh
import sys
import util.helpers as h

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

		f = filelist.readline()
