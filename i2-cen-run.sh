#!/bin/bash

EZ_PATH=./src
N="ez-segway"

cd $EZ_PATH

tmux new-session -d -s $N

tmux new-window -t $N:100 -n 'mininet' "sudo ./topo.py --method central --topo i2"
sleep 1

OFP_PORT=$((6733))
WSAPI_PORT=$((8733))
tmux new-window -t $N:1 -n "ctrl" "TOPO_INPUT=i2 ryu-manager --ofp-tcp-listen-port $OFP_PORT --use-stderr ./central_ctrl.py"

#tmux select-window -t $N:100
#tmux attach-session -t $N
#exit
sleep 5

CTRL_PID=`ps ax | grep central_ctrl.py | grep python | awk '{ print $1 }'`
while ps -p $CTRL_PID > /dev/null; do sleep 1; done;

tmux send-keys -t $N:100 'exit' Enter
sleep 10
tmux list-panes -s -F "#{pane_pid} #{pane_current_command}" | grep -v tmux | awk '{print $1}' | sudo xargs kill


