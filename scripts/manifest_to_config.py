#!/usr/bin/python

# File name: manifest_to_config.py
# Author: Sebastian Hennig
# Date created: 18.07.2018
# Python Version: 2.7
# Description: Parses the Manifest XML file downloaded from GENI and outputs
# the hosts IP addresses in the config file format

import json
import os
import re
import sys
import xml.etree.ElementTree

project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
e = xml.etree.ElementTree.parse(sys.argv[1]).getroot()

manager_ip = ""
ip_array = []
total_node_count = 0
# Iterate through all nodes in the array
for child in e.findall('{http://www.geni.net/resources/rspec/3}node'):
    total_node_count += 1
    # This selects the first IP address found as the manager node IP
    if not manager_ip:
        manager_ip = child.find('{http://www.geni.net/resources/rspec/3}host').attrib['ipv4']
        continue
    ip_array.append(child.find('{http://www.geni.net/resources/rspec/3}host').attrib['ipv4'])


worker_str = "("
for ip in ip_array:
    worker_str += "\"" + ip + "\" "
worker_str += ")"

configPath = os.path.join(project_root, "docker-swarm.cfg")
with open(configPath) as f:
    config_manager = re.sub('manager=.*', 'manager="' + manager_ip + '"', f.read())
    config_workers = re.sub('workers=.*', 'workers=' + worker_str, config_manager)

with open(configPath, "w") as f:
    f.write(config_workers)

applicationConfPath = os.path.join(project_root, "docker-entrypoint.sh")
with open(applicationConfPath) as f:
    applicationConf = re.sub('-Dconstants.gui-endpoint=\"(.*?)\"', '-Dconstants.gui-endpoint="http://' + manager_ip + ':3000"', f.read())

with open(applicationConfPath, "w") as f:
    f.write(applicationConf)

graphPath = os.path.join(project_root, "gui/src/graph.js")
with open(graphPath) as f:
    graph = re.sub('const SERVER = \"(.*?)\"', 'const SERVER = "' + manager_ip + '"', f.read())

with open(graphPath, "w") as f:
    f.write(graph)

constantsPath = os.path.join(project_root, "gui/constants.js")
with open(constantsPath) as f:
    constants = re.sub('const SERVER = \"(.*?)\"', 'const SERVER = "' + manager_ip + '"', f.read())

with open(constantsPath, "w") as f:
    f.write(constants)

applicationConfSourcePath = os.path.join(project_root, "src/main/resources/application.conf")

with open(applicationConfSourcePath) as f:
    minimum_members_line = re.sub(r'min-nr-of-members = \d+', 'min-nr-of-members = ' + str(total_node_count), f.read())
    gui_endpoint_line = re.sub('gui-endpoint = \"(.*?)\"', 'gui-endpoint = "http://' + manager_ip + ':3000"', minimum_members_line)
    ip_array.append(manager_ip)
    ip_addresses_line = re.sub(r'host-ip-addresses = \[.*\]', 'host-ip-addresses = ' + json.dumps(ip_array), gui_endpoint_line)
    disable_mininet_sim_line = re.sub(r'mininet-simulation = .*', 'mininet-simulation = false', ip_addresses_line)
    replace_seed_node_line = re.sub(r' \"akka\.tcp://tcep@10\.0\.0\.253:\"\$\{\?constants\.base-port\}\"\"', ' #\"akka.tcp://tcep@10.0.0.253:\"${?constants.base-port}\"\"', disable_mininet_sim_line)
    replace_seed_node_line = re.sub(r'#\"akka\.tcp://tcep@simulator:\"\$\{\?constants\.base-port\}\"\"', '\"akka.tcp://tcep@simulator:\"${?constants.base-port}\"\"', replace_seed_node_line)

with open(applicationConfSourcePath, "w") as f:
    f.write(replace_seed_node_line)

