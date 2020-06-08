#!/usr/bin/python

# File name: set_ips.py
# Author: Manisha Luthra
# Date created: 11.05.2020
# Python Version: 2.7
# Description: set argument for different algo in docker-stack.yml
# Usage: python set_algo.py <num-algo>

import json
import os
import re
import sys
import xml.etree.ElementTree


project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
algo = sys.argv[1]
query = sys.argv[2]

print "algo: " + algo + "query: " + query

# 1: Relaxation
# 2: Starks
# 8: Rizou
# 9: Producer Consumer
# 10: Global Optimal
# 11: Random
# 7:

dockerStackPath = os.path.join(project_root, "docker-stack.yml")

with open(dockerStackPath) as f:
    mode = re.sub('--mode \d+', '--mode ' + algo, f.read())

with open(dockerStackPath, "w") as f:
    f.write(mode)


with open(dockerStackPath) as f:
    queryStr = re.sub('--query [A-Za-z]+', '--query ' + query, f.read())
    #print "query:"+ queryStr

with open(dockerStackPath, "w") as f:
    f.write(queryStr)