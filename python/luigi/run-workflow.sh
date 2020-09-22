#!/bin/bash

python luigi_mapreduce.py --local-scheduler \
--input-file /user/hduser/input/input.txt \
--output-file /user/hduser/wordcount



python luigi_pig.py --local-scheduler \
--input-file /user/hduser/input/input.txt \
--output-file /user/hduser/output \
--script-path pig/wordcount.pig

