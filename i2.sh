#!/bin/sh
./i2-ez-run.sh
cp logs/i2-global-ctrl.log results/i2/ez-i2.log
./i2-cen-run.sh
cp logs/cen_result.log results/i2/cen-i2.log
