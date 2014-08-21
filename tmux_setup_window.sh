#!/bin/bash
tmux new-window
for ((i = 0; i < 3; i++))
do
	tmux split
done
tmux select-layout even-vertical
for ((i = 1; i <= 4; i++))
do
	tmux send-keys -t:.$i "cd ~/git/mersenne" Enter
done
tmux send-keys -t:.1 "rm -f acceptor?/*" Enter
sleep 2
for ((i = 1; i <= 3; i++))
do
	tmux send-keys -t:.$i "./run.sh $((i - 1))" Enter
done
