A=/tmp/coriolisSelection.C.txt
B=/tmp/coriolisSelection.D.txt
rm ${A}.log ${B}.log
bash loadDB.sh $A  &
pids[0]=$!
bash loadDB.sh $B  &
pids[1]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done