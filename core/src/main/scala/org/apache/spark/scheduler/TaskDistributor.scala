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

import org.apache.spark.internal.Logging

import scala.collection.{Map, mutable}
import scala.collection.mutable.HashMap
import scala.util.Random

/**
 * Created by lxb on 17-11-21.
 */
class TaskDistributor(mode: String)  extends Logging{

  private val distributeMode = mode match {
    case "random" =>
      logInfo("TaskDistribute mode: random")
      DistributeMode.RANDOM

    case "locality" =>
      logInfo("TaskDistribute mode: locality")
      DistributeMode.LOCALITY

    case _ =>
      logInfo("unknown TaskDistribute mode")
      DistributeMode.RANDOM
  }

  def distributeTask(stageId: Int,
      partitionsToCompute: Seq[Int],
      taskIdToLocations: Map[Int, Seq[TaskLocation]],
      partners: HashMap[Int, ClusterInfo]): HashMap[Int, ClusterInfo] = {
    val alivePartners = partners.filter{ case(pid, info) =>
        info.appMasterState == ApplicationMasterState.RUNNING
    }
    logInfo(s"partitions size: ${partitionsToCompute.size}, "
      + s"partners num: ${partners.size}, "
      + s"alive partners num: ${alivePartners.size}")
    if (alivePartners.size == 0) {
      logInfo("partners all died, but that's impossible")
      return partners
    }

    /*
    val partnersId = alivePartners.keySet.toArray
    val subPartitions = partitionsToCompute.groupBy{ p =>
      partnersId((new Random).nextInt(partnersId.length))
    }
    */
    val subPartitions = applyDistributeMode(partitionsToCompute, taskIdToLocations, alivePartners)

    alivePartners.foreach{ case (pid, cInfo) =>
      if (subPartitions.isDefinedAt(pid)) {
        cInfo.setSubPartitions(stageId, subPartitions(pid))
        /*
        if (tasks != null) {
          val st = tasks.filter{t => subPartitions(pid).contains(t.partitionId)}
          cInfo.setSubtasks(stageId, st)
        }
        */
        partners.update(pid, cInfo)
        logInfo(s"partner ${pid} got partitions: ${subPartitions(pid).sorted.toString()}")
      } else {
        cInfo.setSubPartitions(stageId, Seq.empty)
        // cInfo.setSubtasks(stageId, Seq.empty)
        partners.update(pid, cInfo)
        logInfo(s"partner ${pid} got partitions: empty")
      }
    }

    val failedPartners = partners.filter{ case(pid, info) =>
      info.appMasterState != ApplicationMasterState.RUNNING
    }
    failedPartners.foreach {case(pid, cInfo) =>
      cInfo.setSubPartitions(stageId, Seq.empty)
      partners.update(pid, cInfo)
    }


    partners
  }

  private def applyDistributeMode(partitionsToCompute: Seq[Int],
                                  taskIdToLocations: Map[Int, Seq[TaskLocation]],
                                  alivePartners: HashMap[Int, ClusterInfo]): Map[Int, Seq[Int]] = {
    distributeMode match {
      case DistributeMode.RANDOM =>
        randomMode(partitionsToCompute, alivePartners)
      case DistributeMode.LOCALITY =>
        localityMode(partitionsToCompute, taskIdToLocations, alivePartners)
    }

  }

  private def randomMode(partitionsToCompute: Seq[Int],
                         alivePartners: HashMap[Int, ClusterInfo]): Map[Int, Seq[Int]] = {
    logInfo("distribute by random")
    val partnersId = alivePartners.keySet.toArray
    partitionsToCompute.groupBy{ p =>
      partnersId((new Random).nextInt(partnersId.length))
    }
  }

  private def localityMode(partitionsToCompute: Seq[Int],
                           taskIdToLocations: Map[Int, Seq[TaskLocation]],
                           alivePartners: HashMap[Int, ClusterInfo]): Map[Int, Seq[Int]] = {
    logInfo("distribute by locality")
    var randomAssign = 0
    val result = new HashMap[Int, mutable.HashSet[Int]]
    val partnersId = alivePartners.keySet.toArray
    val nodeToClusterId = new mutable.HashMap[String, Int]
    alivePartners.foreach {case(pid, info) =>
      info.nodes.foreach {node => nodeToClusterId(node) = pid}
    }
    partitionsToCompute.map{ p =>
      var assignToCluster = -1
      val locations = taskIdToLocations(p)
      if (locations == Nil) {
        assignToCluster = partnersId((new Random).nextInt(partnersId.length))
        randomAssign += 1
      } else {
        val vote = partnersId.map {pid => (pid, 0)}.toMap
        locations.foreach {l =>
          val cid = if (nodeToClusterId.isDefinedAt(l.host)) {
            nodeToClusterId(l.host)
          } else {
            partnersId((new Random).nextInt(partnersId.length))
          }
          vote.updated(cid, vote(cid) + 1)
        }
        assignToCluster = vote.maxBy(_._2)._1
      }
      result.getOrElseUpdate(assignToCluster, new mutable.HashSet[Int]()) += p
    }
    logInfo(s"assign ${partitionsToCompute.size} tasks, $randomAssign tasks have not data locality")
    result.map {case(cid, partitionsSet) => (cid, partitionsSet.toSeq)}
  }

}

object DistributeMode extends Enumeration {

  type DistributeMode = Value
  val RANDOM, LOCALITY = Value
}
