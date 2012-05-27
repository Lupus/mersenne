#!/bin/bash
PROC_NUM=4

if [ ! -d logs ] ; then
	mkdir logs
fi


for loss in `seq 0 5 90`
do

	sudo tc qdisc del dev lo root
	#sudo tc qdisc add dev lo root netem delay 50s 25ms distribution normal
	if [ $loss!=0 ]; then
		sudo tc qdisc add dev lo root netem loss $loss%
	fi

	truncate --size 0 peers

	for i in `seq 0 $PROC_NUM`
	do
		echo 127.0.0.$(($i+1)) >> peers
	done

	for i in `seq 0 $PROC_NUM`
	do
		sh -c "exec stdbuf -i0 -o0 -e0 ./mersenne $i 2>&1 | tai64n > logs/$i.log &"
	done

	#for log in logs/*.log
	#do
	#	truncate --size 0 $log
	#done

	sleep $((1*60))

	killall mersenne

	echo -en "$loss\t"
	cat logs/*.log | sort | tai64nlocal | ruby plot.rb $PROC_NUM
done
