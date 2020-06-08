import os
import re
import sys


project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
mapek = sys.argv[1]
requirement = sys.argv[2]
query = sys.argv[3]
mode = sys.argv[4]
transitionStrategy = sys.argv[5]
transitionExecutionMode = sys.argv[6]

dockerStackPath = os.path.join(project_root, "docker-stack.yml")

with open(dockerStackPath) as f:
    modeStr = re.sub('--mode \d+', '--mode ' + mode , f.read())
    queryStr = re.sub('--query \w+', '--query ' + query, modeStr)
    mapekStr = re.sub('--mapek \w+', '--mapek ' + mapek, queryStr)
    reqStr = re.sub('--req \w+', '--req ' + requirement, mapekStr)
    transStratStr = re.sub('--transitionStrategy \w+', '--transitionStrategy ' + transitionStrategy, reqStr)
    transExStr = re.sub('-- \w+', '--transitionExecutionMode ' + transitionExecutionMode, transStratStr)

with open(dockerStackPath, "w+") as f:
    f.write(transExStr)

