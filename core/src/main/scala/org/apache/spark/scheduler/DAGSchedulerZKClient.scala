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

import java.util
import java.util.{ArrayList, List}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sparkzk.zkclient.common.serializer.ObTrans
import org.apache.spark.sparkzk.zkclient.{ZkSparkAmInfoManager, ZkSparkDataClient, ZkSparkLongElectorClient}

import scala.collection.{JavaConverters, mutable}

/**
 * Created by lxb on 17-11-28.
 */
class DAGSchedulerZKClient(sc: SparkContext) extends Logging{

  private var zkSparkDataClient: ZkSparkDataClient = null
  private var zkSparkLongElectorClient: ZkSparkLongElectorClient = null
  private var zkSparkAmInfoManager: ZkSparkAmInfoManager = null
  var sparkConf = sc.getConf
  val zkConfStringTag: String = "spark.zk.hosts"
  def register(id: Int, group: String): Unit = {
    zkSparkDataClient = new ZkSparkDataClient(sparkConf.get(zkConfStringTag),
      group, 100)
    zkSparkLongElectorClient = new ZkSparkLongElectorClient(sparkConf.get(zkConfStringTag), group,
      Integer.toString(id), 1.toLong)
    zkSparkAmInfoManager = new ZkSparkAmInfoManager(sparkConf.get(zkConfStringTag), group,
      Integer.toString(id))
  }

  def updateEpoch(epoch: Long): Unit = {
    zkSparkLongElectorClient.setSelectNum(epoch)
  }

  def isLeader(): Boolean = {
    zkSparkLongElectorClient.amIMaster()
  }

  def getLeader(): Int = {
    zkSparkLongElectorClient.getMasterId()
  }

  def getPartners(): mutable.HashSet[Int] = {
    val childrenName: util.List[String] = zkSparkAmInfoManager.getAllNodeName()
    val a : Int = childrenName.size()
    var index: Int = 0
    val chi = new mutable.HashSet[Int]
    for(index <- 0 to a-1) {
      val tempId = Integer.parseInt(childrenName.get(index))
      chi.add(tempId)
    }
    chi
  }

  def updateSyncInfo(info: SyncInfo): Unit = {
    zkSparkDataClient.putData(ObTrans.ObjectToBytes(info))
  }

  def getSyncInfo(): SyncInfo = {
    ObTrans.BytesToObject(zkSparkDataClient.getData()).asInstanceOf[SyncInfo]
  }

  def isSyncInfoUpdate(): Boolean = {
    zkSparkDataClient.isDataFresh()
  }

  def updateStageInfo(jobId: Int, stageId: Int): Unit = {
    zkSparkAmInfoManager.putData(jobId, stageId)
  }

  // Map(nodeId, (jobId, stageId))
  def getStageInfo(): mutable.HashMap[Int, (Int, Int)] = {
    val infoMap = zkSparkAmInfoManager.getNodeDataInfo
    val stageInfo = new mutable.HashMap[Int, (Int, Int)]
    val it = infoMap.keySet().iterator()
    while (it.hasNext()) {
      val nid = it.next()
      val jobId = infoMap.get(nid)(0)
      val stageId = infoMap.get(nid)(1)
      stageInfo(nid) = (jobId, stageId)
    }

    /*
    for (nid <- infoMap.keySet()) {
      val jobId = infoMap.get(nid)(0)
      val stageId = infoMap.get(nid)(1)
      stageInfo(nid) = (jobId, stageId)
    }
    */

    stageInfo
  }

}
