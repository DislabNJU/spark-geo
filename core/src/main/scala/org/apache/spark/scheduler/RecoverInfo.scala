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

import scala.collection.mutable
import scala.collection.mutable.{HashMap, Map}

/**
 * Created by lxb on 17-11-20.
 * @param runningJobId Running job id
 * @param runningStageId Running stage id
 * @param jobResults Success jobs' results
 * @param mapStatuses Latest completed stage's snapshot of mapStatuses
 * @param completionEvents Running stage's completed tasks' results
 */
class RecoverInfo (
    var runningJobId: Int,
    var runningStageId: Int,
    var jobResults: HashMap[Int, HashMap[Int, Any]],
    var mapStatuses: Map[Int, Array[MapStatus]],
    var completionEvents: mutable.HashSet[CompletionEvent]) extends Serializable {

  def this() = this(-1, -1, null, null, null)

  def setJobId(id: Int): Unit = {
    runningJobId = id
  }

  def setStageId(id: Int): Unit = {
    runningStageId = id
  }

  // called on job ends
  def setJobResults(results: HashMap[Int, HashMap[Int, Any]]): Unit = {
    results -= (runningStageId)
    jobResults = results
    runningJobId += 1
  }

  // called on stage ends
  def setMapStatuses(statuses: Map[Int, Array[MapStatus]]): Unit = {
    mapStatuses = statuses
  }

  def setCompletionEvent(events: mutable.HashSet[CompletionEvent]): Unit = {
    completionEvents = events
  }

}
