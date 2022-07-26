# bash testload.sh <updatedprofiles file path>
# meant to run off of last night's /logs/ifremer/synclog-yyymmdd/updatedprofiles
while read i ; do python translateProfile.py $i 2>&1 ; done < $1