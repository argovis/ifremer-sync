# bash testload.sh <working dir> <updatedprofiles file path under working dir>
# meant to run off of last night's /logs/ifremer/synclog-yyymmdd/updatedprofiles,
# or the output of freshrebuild.sh

echo ${1}/${2}

[ -f ${1}/${2}-todo ] && echo 'picking up from interrupt' >> ${1}/${2}-logs
[ ! -f ${1}/${2}-todo ] && cp ${1}/${2} ${1}/${2}-todo
while read i ; 
do 
	python translateProfile.py $i 2>&1 >> ${1}/${2}-logs
	sed -i "1 d" ${1}/${2}-todo
done < ${1}/${2}-todo



