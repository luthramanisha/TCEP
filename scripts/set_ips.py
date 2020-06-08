#!/usr/bin/python

# File name: set_ips.py
# Author: Sebastian Hennig
# Date created: 18.07.2018
# Python Version: 2.7
# Description: Parses the Manifest XML file downloaded from GENI and outputs
# the hosts IP addresses in the config file format
# Usage: python set_ips.py 

import os
import re

project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
#e = xml.etree.ElementTree.parse(sys.argv[1]).getroot()

manager_ip = ""
ip_array = []
total_node_count = 0
n_publisher_nodes = 0

with open(os.path.join(project_root, "docker-swarm.cfg")) as cfgFile:
    for line in cfgFile:
        name, var = line.partition("=")[::2]
        if name == "manager":
            manager_ip = var.rstrip()
        elif name == "workers":
            ip_array = var.split(' ')
        elif name == "n_publisher_nodes_total":
            n_publisher_nodes = var.rstrip()

total_node_count = len(ip_array) + 1 #plus manager

print n_publisher_nodes
####


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

with open(applicationConfSourcePath, "w") as f:
    f.write(minimum_members_line)

#with open(applicationConfSourcePath) as f:
#    nr_of_publisher_nodes_line = re.sub(r'number-of-speed-publisher-nodes = \d+', 'number-of-speed-publisher-nodes = ' + str(n_publisher_nodes - 1), f.read())

#with open(applicationConfSourcePath, "w") as f:
#    f.write(nr_of_publisher_nodes_line)

with open(applicationConfSourcePath) as f:
    gui_endpoint_line = re.sub('gui-endpoint = \"(.*?)\"', 'gui-endpoint = "http://' + manager_ip + ':3000"', f.read())

with open(applicationConfSourcePath, "w") as f:
    f.write(gui_endpoint_line)

#with open(applicationConfSourcePath) as f:
#    ip_array.append(manager_ip)
#    ip_addresses_line = re.sub(r'host-ip-addresses = \[.*\]', 'host-ip-addresses = ' + json.dumps(ip_array), f.read())

#with open(applicationConfSourcePath, "w") as f:
#    f.write(ip_addresses_line)

with open(applicationConfSourcePath) as f:
    disable_mininet_sim_line = re.sub(r'mininet-simulation = .*', 'mininet-simulation = false', f.read())

with open(applicationConfSourcePath, "w") as f:
    f.write(disable_mininet_sim_line)

with open(applicationConfSourcePath) as f:
    replace_seed_node_line = re.sub(r'\"akka.tcp://tcep@10.0.0.253:2500\"', '#\"akka.tcp://tcep@10.0.0.253:2500\"', f.read())

with open(applicationConfSourcePath, "w") as f:
    f.write(replace_seed_node_line)

with open(applicationConfSourcePath, "w") as f:
    f.write(replace_seed_node_line)
