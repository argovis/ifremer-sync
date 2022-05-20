# keep all logging output here
date=$(date '+%Y%m%dT%H%M%S')
logdir=/logs/ifremer/synclog-${date}
mkdir -p $logdir

# download new data and keep a log of what's new
rsync -avzhi --delete --omit-dir-times --no-perms vdmzrs.ifremer.fr::argo/ /ifremer > ${logdir}/rsynclog

# extract set of profiles to CRUD
grep '\/profiles\/.*\.nc' ${logdir}/rsynclog | tr -s ' ' | cut -d ' ' -f 2 | sed 's|^|/ifremer/|g' > ${logdir}/updatedprofiles
python process-rsync-result.py ${logdir}/updatedprofiles
sort /tmp/profileUpdates.txt | uniq > /tmp/temp && mv /tmp/temp ${logdir}/updatedprofiles

# load profiles, with logging
while read i ; do python translateProfile.py $i >> ${logdir}/updatedprofiles.log 2>&1 ; done < ${logdir}/updatedprofiles

# once loading complete, kick off summary precomputation
python summary-computation.py