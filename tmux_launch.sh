#!/bin/bash
for ((i = 1; i <= 3; i++))
do
	tmux send-keys -t:.$i "./run.sh $((i - 1))" Enter
done
