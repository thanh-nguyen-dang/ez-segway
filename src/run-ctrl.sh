#!/bin/bash

switch_id=$1

ctrl_port=$((6733 + $switch_id))

export EZSWITCH_ID=$switch_id
exec ryu-manager --ofp-tcp-listen-port $ctrl_port --use-stderr --verbose ./local_ctrl.py
