# bash testload.sh <updatedprofiles file path>
# meant to run off of last night's /logs/ifremer/synclog-yyymmdd/updatedprofiles

[ -f $1-todo ] && echo 'picking up from interrupt' >> $1-logs
[ ! -f $1-todo ] && cp $1 $1-todo
while read i ; 
do 
	python translateProfile.py $i 2>&1 >> $1-logs
	sed -i "1 d" $1-todo
done < $1-todo