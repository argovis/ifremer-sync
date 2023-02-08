# redo the nightly update in the folder $1; assumes the rsync and `updatedprofiles` generation completed successfully, just redoes mongo load
date=$(date '+%Y%m%dT%H%M%S')
logdir=$1

# redo load profiles, with logging
while read i ; do python translateProfile.py $i >> ${logdir}/updatedprofiles.redo-${date}.log 2>&1 ; done < ${logdir}/updatedprofiles

# once loading complete, kick off summary precomputation
python summary-computation.py