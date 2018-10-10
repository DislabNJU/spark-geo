Introduction
===
What is Houtu?
--- 
Houtu is a geo-distributed analytics system, which is developed with Spark, YARN and Zookeeper as building blocks. It has an interesting decentralized architechure, together with the designed resource mangement and task scheduling algorithms, leading to the geo-distributed jobs runing in it good performance. Additionally, it ensures modest monetary cost when deployed in public cloud. The blew figure is an one-slide overview. See the detailed introduction via the [paper](https://github.com/DislabNJU/Houtu/blob/branch-0.2/Houtu-tech-report.pdf).
<img width="650" src="https://github.com/DislabNJU/Houtu/blob/branch-0.2/aa.PNG"/>
### Why decentralized architechure?
Because data is naturally generated at geo-distributed data centers, anaytics jobs on these data are emerging as a daily requirement. Naive extension of cluster-scale analytics systems to the scale of geo-distributed data centers fails to meet upcoming regulatory constraints, which prevent a master machine from controlling worker machines from remote data centers. An alternative is to deploy an autonomous data analytics system per data center, and extend the functionalities of original system to allow to coordinate for geo distributed jobs, as shown in the above figure.
### Why reduced monetary cost?

How Houtu works?
===
