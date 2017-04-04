#!/bin/sh
./b4-ez-run.sh
cp logs/b4-global-ctrl.log results/b4/ez-b4.log
./b4-cen-run.sh
cp logs/cen_result.log results/b4/cen-b4.log
