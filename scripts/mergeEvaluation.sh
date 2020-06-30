#!/usr/bin/env bash

cat 13-Mai*/MFGS* | sed '/Placement/d' - >  performance_evaluation_conjunction
cat performance_evaluation_conjunction | set '/Accident Detection/Conjunction' - >  performance_evaluation_conjunction
