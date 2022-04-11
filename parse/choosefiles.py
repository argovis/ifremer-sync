# usage: python choosefiles.py
# assumes a first rsync populating fresh content under /ifremer

import os
import util.helpers as h

dacs = os.listdir('/ifremer')
filelist = open("/tmp/profileSelection.txt", "w")

for dac in dacs:
    print('processing DAC: ', dac )
    platforms = os.listdir('/ifremer/'+dac)
    for platform in platforms:
        print('processing platform: ', platform)
        folder = '/ifremer/'+dac+'/'+platform+'/profiles'
        try:
            files = os.listdir(folder)
        except:
            print('warning: /ifremer/'+dac+'/'+platform + ' doesnt have a /profiles dir')
            continue
        profiles = set(map(h.pickprof, files))
        for profile in profiles:
            files = h.select_files(folder, profile)
            if len(files)>0:
                for f in files:
                    filelist.write(f + ' ')
                filelist.write('\n')

filelist.close()