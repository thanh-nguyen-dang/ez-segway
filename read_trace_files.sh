#!/bin/bash

mkdir -p results/parsed
mkdir -p results/b4-cdf
mkdir -p results/b4-cdf/time_new_path
mkdir -p results/i2-cdf
mkdir -p results/i2-cdf/time_new_path
cp results/b4/*.log results
cp results/i2/*.log results
cd simulator;

python ez_tracer.py --dataFolder results
