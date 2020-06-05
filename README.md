# TCEP: Transitions in Operator Placement to Adapt to Dynamic Network Environments

TCEP is a research project that provides a programming model for development of operator placement algorithms for streaming applications and a means to adapt operator placement by supporting transitions. 
 
TCEP contributes the following:

+ **Interface** to implement _operator placement_ algorithms
+ **Transition execution strategies** for a _cost-efficient_ and _seamless_ operator placement transition
+ **Lightweight learning strategy** for selection of operator placement for the given QoS demands
+ **Heterogeneous infrastructure support** for execution of operator graphs

To run TCEP simply do `./scripts/publish-tcep.sh all $user@$IP` 

This project is build on [AdaptiveCEP](https://pweisenburger.github.io/AdaptiveCEP/) for specifying complex events and QoS demands. 

## Getting Started

### Prerequistes 

* Docker v19.03 or later
* JDK8
* sbt
* Docker registry account
* adjust *scripts/templates/docker-swarm.cfg* as indicated in the file (for distributed execution)


