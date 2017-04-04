#!/bin/bash
cp results/update_time.log r-scripts/update_time.log
rm -r r-scripts/time_new_path_b4
rm -r r-scripts/time_new_path_i2
cp -r results/b4-cdf/time_new_path r-scripts/time_new_path_b4
cp -r results/i2-cdf/time_new_path r-scripts/time_new_path_i2
cd r-scripts;

./exp-plotting-percentile.r > exp-plotting-percentile.log
./cdf_flow_update.r > cdf_flow_update.log

