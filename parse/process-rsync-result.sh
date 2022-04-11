# usage: bash process-rsync-result.sh <output of rsync -i>
date=$(date '+%Y%m%dT%H%M%S')
grep '\/profiles\/.*\.nc' $1 | tr -s ' ' | cut -d ' ' -f 2 | sed 's|^|/ifremer/|g' > /tmp/updatedprofiles.${date}.txt
python process-rsync-result.py /tmp/updatedprofiles.${date}.txt
sort /tmp/profileUpdates.txt | uniq > /tmp/temp && mv /tmp/temp /tmp/profileUpdates.txt