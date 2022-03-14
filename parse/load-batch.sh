A=/tmp/bodcSelection.A.txt
B=/tmp/bodcSelection.B.txt
rm ${A}.log ${B}.log
bash loadDB.sh $A  &
pids[0]=$!
bash loadDB.sh $B  &
pids[1]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done