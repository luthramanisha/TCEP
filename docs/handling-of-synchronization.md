# Handling of synchronization #

Akka Cluster provides a fault-tolerant decentralized peer-to-peer based cluster membership service with no single point of failure or single point of bottleneck. It does this using [gossip](http://doc.akka.io/docs/akka/current/scala/common/cluster.html#gossip) protocols and an automatic failure detector [2]. 

The actor registers itself as subscriber of certain cluster events. It receives events corresponding to the current state of the cluster when the subscription starts and then it receives events for changes that happen in the cluster [1].

## Gossip ##
The cluster membership used in Akka is based on Amazon’s [Dynamo](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) system and particularly the approach taken in Basho’s’ Riak distributed database. Cluster membership is communicated using a Gossip Protocol, where the current state of the cluster is gossiped randomly through the cluster, with preference to members that have not seen the latest version.

### How does Gossip Protocol work? ###

These protocols usually work [3] like this:

- A node in the network randomly selects a peer with which it will exchange some information.
- A data exchange process happens between these peers.
- Each node processes the data it received.
- These steps are periodically repeated by the nodes in the network as a way to disseminate information.


## References ##
	
-  [[1]](http://doc.akka.io/docs/akka/current/scala/cluster-usage.html)
-  [[2]](http://doc.akka.io/docs/akka/current/scala/common/cluster.html)  
-  [[3]](http://alvaro-videla.com/2015/12/gossip-protocols.html)