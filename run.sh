#!/bin/bash
PROC_NUM=10

if [ ! -d logs ] ; then
	mkdir logs
fi

sudo tc qdisc del dev lo root
sudo tc qdisc add dev lo root netem delay 120ms 50ms distribution normal
sudo tc qdisc change dev lo root netem loss 15% 25%

truncate --size 0 peers

for i in `seq 0 $PROC_NUM`
do
	echo 127.0.0.$(($i+1)) >> peers
done

for i in `seq 0 $PROC_NUM`
do
	sh -c "exec stdbuf -i0 -o0 -e0 ./mersenne $i 2>&1 | tai64n > logs/$i.log &"
done

sleep 20

killall mersenne

cat logs/*.log | sort | tai64nlocal | ruby plot.rb $PROC_NUM

