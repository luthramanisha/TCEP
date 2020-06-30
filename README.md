# TCEP

TCEP is a research project that provides a programming model for development of operator placement algorithms for streaming applications and a means to adapt operator placement by supporting transitions. 
 
TCEP contributes the following:

+ **Programming model** to implement _operator placement_ algorithms
+ **Transition execution strategies** for a _cost-efficient_ and _seamless_ operator placement transition
+ **Lightweight learning strategy** for selection of operator placement for the given QoS demands
+ **Heterogeneous infrastructure support** for execution of operator graphs

To run TCEP simply do `./scripts/build.sh && docker-compose up`. Check our [live demo](http://35.246.223.49:3000/) at Google cloud! 

[Getting Started](#getting-started)

[Publications](#publications)

[Acknowledgement](#acknowledgement)

[Contact](#contact)

## [Getting Started](#getting-started)

### Prerequisites 

* Docker v19.03 or later and a registry account
* JDK8
* sbt

### Running on a central server (centralized execution)

Simply build TCEP and run using docker-compose `./scripts/build.sh && docker-compose up`

### Running cluster simulation (distributed execution)

Adjust `$PROJECT_HOME/scripts/templates/docker-swarm.cfg` as indicated in the file 

Simulation can be executed using a GUI (mode 7) or without a GUI. The mode can be set in `$PROJECT_HOME/docker-stack.yml` file as an environment variable in simulator service.

A full list of simulations can be found below:

| Mode  | Description  |
|---|---|
| 1 | Test Relaxation algorithm  |
| 2 | Test MDCEP algorithm |
| 3 | Test SMS transition |
| 4 | Test MFGS transition |
| 5 | unavailable |
| 6 | Do nothing |
| 7 | Test with GUI |
| 8 | Test Rizou |
| 9 | Test Producer Consumer |
| 10 | Test Optimal |
| 11 | Test Random |
| 12 | Test Lightweight |

`./scripts/publish-tcep.sh all $user@$IP`

the script will deploy the docker stack defined by docker-stack.yml to the cluster formed by the VMs

### Running using GENI/ CloudLab VMs
GENI and CloudLab provides a large-scale experiment infrastructures where users can obtain computing instances throughout the world to perform network experiments.
TCEP includes useful scripts to enable easier simulations on GENI which are described below.

```
cd scripts/
```

First, a so called RSpec XML file is needed in order to get a GENI/CloudLab cluster up and running. To automatically generate a cluster with a specified number of nodes you can execute the following command:

```
python generate_geni_rspec.py {number-of-nodes} {out-directory}
```

This will generate the rspec.xml file with the at "out-directory" with the specified number of nodes. Furthermore, this also generate the Docker swarm file (docker-swarm.cfg) with the correct amount of empty apps running on the GENI hosts.

After you deployed the RSpec on GENI, you can download a Manifest XML file which contains information of the hosts that GENI deployed. This is useful because GENI automatically generates IP addresses for the hosts and if you created a cluster with a high amount of nodes the search for this IP addresses can be a heavy overhead.
After downloading the manifest file you can run the following command to extract the IP addresses out of it and print out the config that can be put into the "docker-swarm.cfg" file:

```
python manifest_to_config.py {manifest-path}
```

This will convert the manifest and will print out the IP addresses of the started hosts in the config format for the docker deploy script.
You should see an output like this

```
manager=72.36.65.68
workers=("72.36.65.69" "72.36.65.70")
```

Now the hosts are successfully deployed on GENI and the project is ready to be setup on the hosts. To setup the GENI instances to be able to run docker, run the following command

```
./publish_tcep.sh setup
```

Note that you maybe have to enter `yes` in the console multiple times to allow SSH connections to the hosts

If you are running on a new cluster you never worked on before, you will maybe need to authorize docker on the master node to be authorized to access a private docker registry. You can do this by executing the following on the master node

```
docker login
```

After the instances are all setup, you can go forward and finally run the cluster on the hosts by executing the following command

```
./publish_tcep.sh all
```

#### Differences to CloudLab

The steps from above are the same, except that you have to replace the name of the RSpec image to be used before calling the generate script.

Specifically, you have to replace this line in scripts/templates/rspec_node.xml 

``` 
<disk_image xmlns="http://www.geni.net/resources/rspec/3" name="urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU16-64-STD"/>
```

with this line

``` 
<disk_image xmlns="http://www.geni.net/resources/rspec/3" name="urn:publicid:IDN+utah.cloudlab.us+image+schedock-PG0:docker-ubuntu16:0"/>
```

This project is based on [AdaptiveCEP](https://pweisenburger.github.io/AdaptiveCEP/) for specifying complex events and QoS demands. 

## [Publications](#publications)

+ M. Luthra, B. Koldehofe, R. Arif, P. Weisenburger, G. Salvaneschi, TCEP: Adapting to Dynamic User Environments by Enabling Transitions between Operator Placement Mechanisms. In Proceedings of the 12th ACM International Conference on Distributed and Event-based Systems (DEBS ’18), pp. 136–147. <a href="https://doi.org/10.1145/3210284.3210292" target="_blank">https://doi.org/10.1145/3210284.3210292</a>
+ P. Weisenburger, M. Luthra, B. Koldehofe and G. Salvaneschi, Quality-Aware Runtime Adaptation in Complex Event Processing. In IEEE/ACM 12th International Symposium on Software Engineering for Adaptive and Self-Managing Systems (SEAMS '17), pp. 140-151. <https://doi.org/10.1109/SEAMS.2017.10>
+ M. Luthra, B. Koldehofe, R. Steinmetz, Transitions for Increased Flexibility in Fog Computing: A Case Study on Complex Event Processing. Informatik Spektrum 42, pp. 244–255 (2019). <https://doi.org/10.1007/s00287-019-01191-0>
+ M. Luthra and B. Koldehofe, ProgCEP: A Programming Model for Complex Event Processing over Fog Infrastructure. In Proceedings of the 2nd International Workshop on Distributed Fog Services Design (DFSD ’19). <https://doi.org/10.1145/3366613.3368121>


## [Acknowledgement](#acknowledgement)

This work has been co-funded by the German Research Foundation (DFG) within the [Collaborative Research Center (CRC) 1053 -- MAKI](https://www.maki.tu-darmstadt.de/sfb_maki/ueber_maki/index.en.jsp).

## [Contact](#contact)

Feel free to contact [Manisha Luthra](https://www.kom.tu-darmstadt.de/kom-multimedia-communications-lab/people/staff/manisha-luthra/) or [Boris Koldehofe](https://www.rug.nl/staff/b.koldehofe/) for any questions. 


