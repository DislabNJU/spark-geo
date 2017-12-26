/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ApplicationMasterRole.ApplicationMasterRole
import org.apache.spark.scheduler.ApplicationMasterState.ApplicationMasterState

import scala.collection.mutable


/**
 * Created by lxb on 17-11-20.
 */
class ClusterInfo () extends Serializable {

  var clusterId: Int = -1
  var clusterName: String = "unset"
  var driverUrl: String = "unset"
  var endpoint: RpcEndpointRef = null
  var appMasterRole: ApplicationMasterRole = null
  var appMasterState: ApplicationMasterState = null
  var subPartitions: mutable.HashMap[Int, Seq[Int]] = mutable.HashMap.empty
  // var subTasks: mutable.HashMap[Int, Seq[Task[_]]]
  var nodes: mutable.HashSet[String] = mutable.HashSet.empty

  // def this() = this(-1, "unset", "unset", null, null, null,
  //   mutable.HashMap.empty, mutable.HashMap.empty)

  def setClusterId(id: Int): Unit = {
    clusterId = id
  }

  def setDriverUrl(url: String): Unit = {
    driverUrl = url
  }


  def setClusterName(host: String): Unit = {
    clusterName = host
  }
/*
  def setNameNodeHost(host: String): Unit = {
    nameNodeHost = host
  }
  */

  def setEndpointRef(ep: RpcEndpointRef): Unit = {
    endpoint = ep
  }

  def setAppMasterRole(role: ApplicationMasterRole): Unit = {
    appMasterRole = role
  }

  def setAppMasterState(state: ApplicationMasterState): Unit = {
    appMasterState = state
  }

  def setSubPartitions(stageId: Int, p: Seq[Int]): Unit = {
    subPartitions(stageId) = p
  }

  def clearSubPartitions(): Unit = {
    subPartitions = subPartitions.map {case (sid, subp) => (sid, Seq.empty)}
  }

  /*
  def setSubtasks(stageId: Int, t: Seq[Task[_]]): Unit = {
    subTasks(stageId) = t
  }
  */

  def setNodes(n: mutable.HashSet[String]): Unit = {
    nodes = n
  }

}
