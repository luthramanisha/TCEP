# TCEP: Transitions in Operator Placement to Adapt to Dynamic Network Environments

TCEP is a research project that provides a programming model for development of operator placement algorithms for streaming applications and a means to adapt operator placement by supporting transitions. 
 
TCEP contributes the following:

+ **Interface** to implement _operator placement_ algorithms
+ Following state-of-the-art placement mechanisms are _provided_ by the system currently:
    + **Relaxation**: Pietzuch, P., Ledlie, J., Shneidman, J., Roussopoulos, M., Welsh, M., & Seltzer, M. (2006). Network-aware operator placement for stream-processing systems. Proceedings - International Conference on Data Engineering, 2006, 49. https://doi.org/10.1109/ICDE.2006.105
    + **MDCEP**: Starks, F., & Plagemann, T. P. (2015). Operator placement for efficient distributed complex event processing in MANETs. 2015 IEEE 11th International Conference on Wireless and Mobile Computing, Networking and Communications, WiMob 2015, 83–90. https://doi.org/10.1109/WiMOB.2015.7347944
    + **Rizou**: Rizou, S., Dürr, F., & Rothermel, K. (2010). Solving the multi-operator placement problem in large-scale operator networks. Proceedings - International Conference on Computer Communications and Networks, ICCCN. https://doi.org/10.1109/ICCCN.2010.5560127
    + **OptimalBDP**: single-node placement that yields minimal BDP
    + **Producer-Consumer**: stream operators on their publisher nodes, all other on consumer/simulator node
    + **Random**: randomly places the operators on the nodes
+ **Transition execution strategies** for a _cost-efficient_ and _seamless_ operator placement transition
+ **Lightweight learning strategy** for selection of operator placement for the given QoS demands


This project is build on AdaptiveCEP [1] for specifying complex events and QoS demands. 

