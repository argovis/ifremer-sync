# usage: python choosefiles.py
# assumes rsync populated content under /ifremer

import os, glob, re
import util.helpers as h

REprefix = re.compile('^[A-Z]*')                 # SD, SR, BD, BR, D or R
REgroup = re.compile('[0-9]*_[0-9]*D{0,1}\.nc')  # everything but the prefix

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
            # extract a list of filenames corresponding to this profile, and parse out the set of prefixes to consider
            pfilenames = [ x.split('/')[-1] for x in glob.glob(folder + '/*_' + profile + '.nc')]
            groupname = REgroup.search(pfilenames[0]).group(0)
            prefixes = [REprefix.match(x).group(0) for x in pfilenames]
            # choose by prefix
            selected_prefixes = h.choose_prefix(prefixes)
            # write to output fo pick up in next script
            if len(selected_prefixes)>0:
                for sp in selected_prefixes:
                    filelist.write(folder + '/' + sp + groupname + ' ')
                filelist.write('\n')

filelist.close()