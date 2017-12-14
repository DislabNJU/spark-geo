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

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.util.Random

/**
 * Created by lxb on 17-11-21.
 */
class TaskDistributor  extends Logging{
  private val distributeMode = DistributeMode.RANDOM

  def distributeTask(stageId: Int,
      partitionsToCompute: Seq[Int],
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
    val subPartitions = applyDistributeMode(partitionsToCompute, alivePartners)

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
        logInfo(s"partner ${pid} got partitions: ${subPartitions(pid).toString()}")
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
                                  alivePartners: HashMap[Int, ClusterInfo]): Map[Int, Seq[Int]] = {
    distributeMode match {
      case DistributeMode.RANDOM =>
        randomMode(partitionsToCompute, alivePartners)
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

}

object DistributeMode extends Enumeration {

  type DistributeMode = Value
  val RANDOM, LOCALITY = Value
}
