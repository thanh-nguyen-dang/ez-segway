#!/bin/bash

EZ_PATH=/home/ubuntu/ez-segway/src

python $EZ_PATH/global_ctrl.py  \
	--logFolder logs\
	--logFile global-ctrl.log\
	--logLevel INFO\
	--data_folder data\
	--topology $1 \
	--topology_type adjacency\
	--method p2p\
	--generating_method random\
	--number_of_flows 0\
	--failure_rate 0.5\
	--repeat_time 50\
	--skip_deadlock 0
