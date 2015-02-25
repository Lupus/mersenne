#!/bin/bash
set -e
for acc_dir in acceptor?
do
	maxsnap="00000000000000000000"
	pushd $acc_dir/snap > /dev/null
	for snap in $(find . -iname '*.snap'| cut -c 3-)
	do
		lsn="${snap%.*}"
		if [ "$lsn" -gt "$maxsnap" ] ; then
			maxsnap=$lsn
		fi
	done
	echo "Greatest snapshot is $maxsnap"
	popd > /dev/null
	pushd $acc_dir/wal > /dev/null
	maxwal="00000000000000000000"
	for wal in $(find . -iname '*.wal'| cut -c 3-)
	do
		lsn="${wal%.*}"
		if [ "$lsn" -lt "$maxsnap" -a "$lsn" -gt "$maxwal" ] ; then
			maxwal=$lsn
		fi
	done
	echo "Maximum wal lower than snapshot is $maxwal"
	if [ $maxwal -eq 0 ] ; then
		maxwal=$maxsnap
	fi
	if [ $maxwal -gt 0 ] ; then
		for file in $(find . -iname '*.wal' -or -iname '*.snap' | cut -c 3-)
		do
			lsn="${file%.*}"
			if [ "$lsn" -lt "$maxwal" ] ; then
				echo "Removing $file"
				rm $file
			fi
		done
	fi
	popd > /dev/null
done
