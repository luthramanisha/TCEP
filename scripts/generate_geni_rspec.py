#!/usr/bin/python

# File name: generate_geni_spec.py
# Author: Sebastian Hennig
# Date created: 18.07.2018
# Python Version: 2.7
# Description: Generates the RSpec and Docker Swarm files that are needed for execution of the simulation
# Usage: python generate_geni_rspec.py {number-of-nodes} {out-directory}

import os
import re
import sys

project_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

image_user = False
image_name = False
gui_image = False
n_speed_streams = 0
n_density_streams = 0

with open(os.path.join(project_root, "docker-swarm.cfg")) as cfgFile:
    for line in cfgFile:
        name, var = line.partition("=")[::2]
        if name == "registry_user":
            image_user = var.rstrip()
        elif name == "tcep_image":
            image_name = var.rstrip()
        elif name == "gui_image":
            gui_image = var.rstrip()
        elif name == "n_speed_streams":
            n_speed_streams = int(var)
        elif name == "n_density_streams":
            n_density_streams = int(var)



####
# CONSTANTS
####
ntp_container = "nserver"
master_hostname = "node0"
tcep_image = image_user + "/" + image_name
gui_image = image_user + "/" + gui_image
n_nodes= int(sys.argv[1])
n_publisher_nodes_needed = n_speed_streams + n_density_streams
n_workers = max(1, n_nodes - n_publisher_nodes_needed - 1)
n_publisher_nodes_total = n_nodes - n_workers - 1
n_speed_publisher_nodes = n_publisher_nodes_total - 1

with open(os.path.join(project_root, "src/main/resources/application.conf")) as f:
    speed_publisher_base_port = int(re.compile(r'base-port = \d+').search(f.read()).group(0).split(' = ')[1]) + 1

density_publisher_node_port = speed_publisher_base_port + n_publisher_nodes_total - 1

assert n_publisher_nodes_total >= 2 , "need at least two publisher nodes (one density and one or more speed)"
assert n_speed_publisher_nodes > 0, "need at least one speed publisher node"
assert n_workers > 0, "need at least one worker"
assert n_workers + n_publisher_nodes_total + 1 == n_nodes, "total number of nodes does not match sum of workers, publishers and subscriber"

# update number of publisher nodes in docker-swarm.cfg (used later by other scripts)
cfgPath = os.path.join(project_root, "docker-swarm.cfg")
with open(cfgPath) as f:
    cfg = re.sub('n_publisher_nodes_total=\d+', 'n_publisher_nodes_total=%i' % n_publisher_nodes_total, f.read())

with open(cfgPath, "w") as f:
    f.write(cfg)

# update number of publisher nodes in application.conf
applicationConfSourcePath = os.path.join(project_root, "src/main/resources/application.conf")
with open(applicationConfSourcePath) as f:
    minimum_members_line = re.sub(r'number-of-speed-publisher-nodes = \d+', 'number-of-speed-publisher-nodes = %i' % n_speed_publisher_nodes, f.read())

with open(applicationConfSourcePath, "w") as f:
    f.write(minimum_members_line)


# Read GENI wrapper template
template_file = open(os.path.join(project_root, "scripts/templates/geni_rspec.xml"), "r")
template = template_file.read()
template_file.close()

# Read GENI node template
nodes = ""
template_node_file = open(os.path.join(project_root, "scripts/templates/rspec_node.xml"), "r")
template_node = template_node_file.read()
template_node_file.close()

# Add nodes with ascending IDs
for i in range(0, int(sys.argv[1])):
    node = template_node
    node = node.replace("{{node-id}}", ("node-" + str(i)))
    nodes += node

template = template.replace("{{nodes}}", nodes)
if not os.path.exists(sys.argv[2]):
    os.mkdir(sys.argv[2])

# Write the GENI RSpec file
out_file = open(sys.argv[2] + "/rspec-" + sys.argv[1] +  ".xml", "w")
out_file.write(template)
out_file.close()

file = open(os.path.join(project_root, "scripts/templates/docker-stack.yml"), "r")
docker_stack = file.read()
file.close()

# NTP Server
file = open(os.path.join(project_root, "scripts/templates/ntpserver-docker.yml"), "r")
ntp_server = file.read()\
    .replace("{{name}}", ntp_container)\
    .replace("{{inport}}", "2200")\
    .replace("{{outport}}", "2200")\
    .replace("{{hostname}}", master_hostname)
file.close()

# Simulator node
file = open(os.path.join(project_root, "scripts/templates/simulator-docker.yml"), "r")
simulator_server = file.read()\
    .replace("{{name}}", "simulator")\
    .replace("{{inport}}", "2500")\
    .replace("{{outport}}", "2500") \
    .replace("{{tcepinport}}", "25001") \
    .replace("{{tcepoutport}}", "25001") \
    .replace("{{image}}", tcep_image) \
    .replace("{{hostname}}", master_hostname)
file.close()

# Consumer node
file = open(os.path.join(project_root, "scripts/templates/consumer-docker.yml"), "r")
consumer_node = file.read() \
    .replace("{{name}}", "consumer") \
    .replace("{{inport}}", "2700") \
    .replace("{{kind}}", "Accident") \
    .replace("{{image}}", tcep_image) \
    .replace("{{hostname}}", master_hostname)
file.close()

# GUI node
file = open(os.path.join(project_root, "scripts/templates/gui-docker.yml"), "r")
gui_server = file.read() \
    .replace("{{name}}", "gui") \
    .replace("{{inport}}", "3000") \
    .replace("{{outport}}", "3000") \
    .replace("{{image}}", gui_image) \
    .replace("{{hostname}}", master_hostname)
file.close()

# Emptyapp node
file = open(os.path.join(project_root, "scripts/templates/emptyapp-docker.yml"))
emptyapp_node = file.read()
file.close()

# Publisher node template
file = open(os.path.join(project_root, "scripts/templates/publisher-docker.yml"))
publisher_node = file.read()
file.close()

publisher_nodes = ""
# we need to generate separate speedpublisher services so each publishes different traces
for i in range(0, n_speed_publisher_nodes):
    node = publisher_node \
        .replace("{{name}}", ("speedPublisher%i" % (i + 1))) \
        .replace("{{inport}}", str(speed_publisher_base_port + i)) \
        .replace("{{outport}}", str(speed_publisher_base_port + i)) \
        .replace("{{image}}", tcep_image) \
        .replace("{{hostname}}", "node%i" % (i + 1)) \
        .replace("{{publisher_kind}}", "SpeedPublisher") \
        .replace("{{publisher_id}}", str(i + 1))
    publisher_nodes += node

node = publisher_node \
    .replace("{{name}}", ("densityPublisher")) \
    .replace("{{inport}}", str(density_publisher_node_port)) \
    .replace("{{outport}}", str(density_publisher_node_port)) \
    .replace("{{image}}", tcep_image) \
    .replace("{{hostname}}", "node%i" % (n_publisher_nodes_total)) \
    .replace("{{publisher_kind}}", "DensityPublisher") \
    .replace("{{publisher_id}}", str(0))

publisher_nodes += node

# emptyapps are spread evenly across available nodes labeled 'worker'
emptyapp_nodes = emptyapp_node\
    .replace("{{name}}", ("worker") )\
    .replace("{{inport}}", str(3400))\
    .replace("{{outport}}", str(3400)) \
    .replace("{{image}}", tcep_image) \
    .replace("{{n_replicas}}", str(n_workers))


# Concatenate all specifications and output to file
containers = ntp_server + gui_server + simulator_server + consumer_node + publisher_nodes + emptyapp_nodes
docker_stack = docker_stack.replace("{{containers}}", containers)

file = open(sys.argv[2] + "/docker-stack-" + sys.argv[1] +  ".yml", "w")
file.write(docker_stack)
file.close()

print("Generated successfully with " + str(n_nodes) + " nodes (" + str(n_publisher_nodes_total) + " publishers, " + str(n_workers) + " workers, 1 simulator) at " + sys.argv[2] + " with image name\n\n" +tcep_image + "\n")

file = open(os.path.join(project_root, "docker-stack.yml"), "w")
file.write(docker_stack)
file.close() 

print("Copied docker-stack.yml and rspec-%i.xml to the projects root directory to be used for the experiment" % n_nodes)


