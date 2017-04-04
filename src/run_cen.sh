#!/bin/bash

EZ_PATH=/home/ubuntu/ez-segway/src
N="ez-segway"

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
  exit 1
fi

tmux new-session -d -s $N

tmux new-window -t $N:100 -n 'mininet' "sudo $EZ_PATH/topo.py --method central --topo $1"
sleep 1
tmux send-keys -t $N:100 'h2 ping h3' Enter

OFP_PORT=$((6733))
WSAPI_PORT=$((8733))
tmux new-window -t $N:1 -n "ctrl" "TOPO_INPUT=$1 ryu-manager --ofp-tcp-listen-port $OFP_PORT --use-stderr --verbose $EZ_PATH/central_ctrl.py"

tmux select-window -t $N:100
tmux attach-session -t $N

