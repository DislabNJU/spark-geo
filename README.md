Introduction
===
What is Houtu?
--- 
Houtu is a geo-distributed analytics system, which is developed with Spark, YARN and Zookeeper as building blocks. It has an interesting decentralized architechure, together with the designed resource mangement and task scheduling algorithms, leading to the geo-distributed jobs runing in it good performance. Additionally, it ensures modest monetary cost when deployed in public cloud. The blew figure is an one-slide overview. See the detailed introduction via the [paper](https://github.com/DislabNJU/Houtu/blob/branch-0.2/Houtu-tech-report.pdf).
<img width="650" src="https://github.com/DislabNJU/Houtu/blob/branch-0.2/architecure.PNG"/>
### Why decentralized architechure?
Because data is naturally generated at geo-distributed data centers, anaytics jobs on these data are emerging as a daily requirement. Naive extension of cluster-scale analytics systems to the scale of geo-distributed data centers fails to meet upcoming regulatory constraints, which prevent a master machine from controlling worker machines from remote data centers. An alternative is to deploy an autonomous data analytics system per data center, and extend the functionalities of original system to allow to coordinate for geo distributed jobs, as shown in the above figure.
### Why reduced monetary cost?
The cost reduction comes from two sources: WAN cost and machine cost. The WAN cost reduction is due to the task scheduling algorithm, which is sketched in next section. The machine cost reduction is due to the usage of Spot instances. The Spot instances have significantly lower price than the On-demand instances, but lose the reliability guarantees. Thus, it is necessary to guarantee reliable job executions in a systemic way. What we do is saving the current job's state and the termination of a Spot instance does not interrupt the job's execution. We carefully design what need to be included in the job’s intermediate information, as shown in the blew figure.

<img width="400" src="https://github.com/DislabNJU/Houtu/blob/branch-0.2/job-log-topo.PNG"/>

How Houtu works?
=== 
Two main steps in a job's execution is step 4 and step 6 in the first figure, which corresponds to managing resources for a job and task scheduling in a job. 

### Resource management
For each job, there is a job manager (namely AM, or Driver) in each data center. These job managers <i>independently</i> manage their resources in their local data center. Here, three cases are classified where a job manager may request more resources from its local master, or maintain its current resources, or release some of it resources. The decision is made according to 

### Task scheduling
