#!/bin/bash

cd simulator;

python flow_change_gen.py  \
	--logFolder logs\
	--logFile data-generator.log\
	--logLevel DEBUG\
	--data_folder data\
	--topology i2\
	--topology_type adjacency\
	--generating_method random\
	--number_of_tests 1000\
	--number_of_flows 1000\
    --seed 19831129
