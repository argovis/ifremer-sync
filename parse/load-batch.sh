bash loadDB.sh /tmp/csiroSelection.A.txt  &
pids[0]=$!
bash loadDB.sh /tmp/csiroSelection.B.txt  &
pids[1]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done