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

import java.lang
import java.{lang => jl}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ApplicationMasterState.ApplicationMasterState
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by lxb on 17-11-17.
 */
class SyncInfo () extends Serializable {

  var groupName: String = "default"
  var jobId: Int = -1
  var stageId: Int = -1
  var rdd: RDD[_] = null
  var partitions: Seq[Int] = Seq.empty
  var partners: HashMap[Int, org.apache.spark.scheduler.ClusterInfo] = mutable.HashMap.empty
  var subPartFlag: (Int, Int) = (-1, -1)
  var stageSeq: mutable.HashMap[Int, Int] = mutable.HashMap.empty
  var originalsRegisterSet: mutable.HashSet[AccumulatorV2[_, _]] = null
  var forTest: Seq[Int] = Seq.empty


  def addTestNum(n: Int): Unit = synchronized{
    forTest :+= (n)
  }

  def setGroup(group: String): Unit = {
    groupName = group
  }

  def setJobId(id: Int): Unit = {
    jobId = id
  }

  def setStageId(id: Int): Unit = {
    stageId = id
  }

  def setRDD(r: RDD[_]): Unit = {
    rdd = r
  }

  def setPartitions(p: Seq[Int]): Unit = {
    partitions = p
  }

  def setPartners(p: HashMap[Int, ClusterInfo]): Unit = {
    partners = p
  }

  def setFollowerState(followerId: Int, state: ApplicationMasterState): Unit = {
    val follower = partners.apply(followerId)
    follower.setAppMasterState(state)
    partners.update(followerId, follower)
  }

  def setSubPartFlag(jobId: Int, stageId: Int): Unit = {
    subPartFlag = (jobId, stageId)
  }

  def addStage(stageIdx: Int, stageId: Int): Unit = {
    stageSeq.put(stageIdx, stageId)
  }

  def setOriginals(o: mutable.HashSet[AccumulatorV2[_, _]]): Unit = {
    originalsRegisterSet = o
  }

  def getLeaderInfo(): (Int, RpcEndpointRef, String) = {
    partners.foreach{ case(id, info) =>
        if (info.appMasterRole == ApplicationMasterRole.LEADER) {
          return (id, info.endpoint, info.driverUrl)
        }
    }
    (-1, null, "unset")
  }

  def changeLeader(leaderId: Int, driverUrl: String): Unit = {
    val (oldLeader, endpoint, url) = getLeaderInfo()
    val info = partners(oldLeader)
    info.setAppMasterRole(ApplicationMasterRole.FOLLOWER)
    info.setAppMasterState(ApplicationMasterState.FAILED)
    partners.update(oldLeader, info)

    val newLeaderInfo = partners(leaderId)
    newLeaderInfo.setAppMasterRole(ApplicationMasterRole.LEADER)
    newLeaderInfo.setDriverUrl(driverUrl)
    partners.update(leaderId, newLeaderInfo)
  }

  def addPartition(stageId: Int, pid: Int, partition: Seq[Int]): Unit = {
    val info = partners(pid)
    info.setSubPartitions(stageId, info.subPartitions(stageId) ++ partition)
  }

}

private object SyncInfo {
  val SLOCK = new Object()

}
