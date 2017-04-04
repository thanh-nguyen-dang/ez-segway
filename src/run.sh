#!/bin/bash

EZ_PATH=/home/ubuntu/ez-segway/src
N="ez-segway"

tmux new-session -d -s $N

n=2
if [ "$1" == "triangle" ]
then
  n=2
elif [ "$1" == "b4" ]
then
  n=11
elif [ "$1" == "i2" ]
then
  n=15
elif [ "$1" == "ex" ]
then
  n=7
elif [ "$1" == "6462" ]
then
  n=21
fi

tmux new-window -t $N:100 -n 'mininet' "sudo $EZ_PATH/topo.py --method p2p --topo $1"
sleep 1
tmux send-keys -t $N:100 'h2 ping h1' Enter

for i in `seq 0 $n`; do
  OFP_PORT=$((6733+$i))
  WSAPI_PORT=$((8733+$i))

  tmux new-window -t $N:$(($i+1)) -n "sw$i" "EZSWITCH_ID=$i TOPO_INPUT=$1 ryu-manager --ofp-tcp-listen-port $OFP_PORT --wsapi-port $WSAPI_PORT --use-stderr --verbose $EZ_PATH/local_ctrl.py"
done

tmux new-window -t $N:101 -n "controller"
tmux send-keys -t $N:101 "sleep 10 && $EZ_PATH/run-global-ctrl.sh $1" Enter

tmux select-window -t $N:100
tmux attach-session -t $N

