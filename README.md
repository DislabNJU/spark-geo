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
Two main steps in a job's execution are step 4 and step 6 in the first figure, which corresponds to managing resources for a job and task scheduling in a job. 

### Resource management
For each job, there is a job manager (namely AM, or Driver) in each data center. These job managers <i>independently</i> manage their resources in their local data center. Here, three cases are classified where a job manager may request more resources from its local master, or maintain its current resources, or release some of it resources. The decision is made according to current resource usage, as well as the presence of waiting tasks. The primary code of our changes is in [this](https://github.com/DislabNJU/Spark/blob/branch-2.0/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala).

### Task scheduling
In normal operation, there is only one <i>primary</i> job manager for a job, and the other job managers are <i>semi-active</i> job managers. When a stage of tasks becomes available, the primary job manager initially decides the fraction of tasks to place on each data center to be proportional to the amount of input data on the data center. After the initial task schedule, the job managers begin to  <i>independently</i> assign the tasks in their local cluster. However, it is common that some resources (WAN) become bottleneck that delays the job execution. In that case, we allow job managers from other data centers ``steal'' tasks from the bottleneck data center. The primary code of this part is in [this](https://github.com/DislabNJU/Houtu/blob/branch-0.2/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala).

The task scheduling method balances the WAN monetary cost and job performance for two reasons. Firstly the initial assignment pushes large number of tasks going to the data center with large input data, avoiding massive data crossing data center boundaries through WAN. Secondly a steal is successful only when tasks in victim JMs have waited for longer (enough) time than those in thief JMs. This may lead additional data going through WAN, but we believe it is <i>worthwhile</i> since moving tasks from bottleneck can effectively reduce job response time.
