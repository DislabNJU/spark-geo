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

import java.io.{IOException, NotSerializableException}
import java.net.ConnectException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.{JavaConversions, Map, mutable}
import scala.collection.mutable.{HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.{ControlThrowable, NonFatal}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcEndpointAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sparkzk.zkclient.{ZkSparkAmInfoManager, ZkSparkRecoveryCentre}
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._
import org.eclipse.jetty.io.EndPoint

import scala.io.Source
import scala.xml.XML

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs (MappedRDD, FilteredRDD, etc).
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  private[scheduler] val shuffleToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  private[scheduler] val waitingStagesWontClear = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)


  private var syncEpoch: Long = 0

  protected val syncEpochLock = new AnyRef

  private var lastJobId = -1

  // completion events epoch
  private var ceEpoch: Long = 0

  private val eventsFromLeader = new mutable.HashMap[Long, HashSet[CompletionEvent]]

  // completion events received from followers during current epoch
  private val eventsFromFollower = new mutable.HashMap[Int, HashSet[CompletionEvent]]

  // completion events received from local (during current epoch on leader)
  private val eventsFromLocal = new HashSet[CompletionEvent]

  private val eventsFromLocalOnLeader = new HashSet[CompletionEvent]

  private val eventsFromLocalOnFollower = new HashSet[CompletionEvent]

  private val eventsToSendToLeader = new HashSet[CompletionEvent]

  private val eventsHandled = new HashSet[CompletionEvent]

  private val eventsToHandle = new HashSet[CompletionEvent]

  private val eventsSuccess = new HashSet[Int]

  protected val successLock = new AnyRef

  protected val eventsToHandleLock = new AnyRef

  // lock eventsFromFollower
  protected val followerLock = new AnyRef

  // lock eventsFromLocal
  protected val localLock = new AnyRef

  // eventsToSendToLeader
  protected val toLeaderLock = new AnyRef

  // ecEpoch to events
  private val historyEventsByEpoch = new mutable.HashMap[Long, HashSet[CompletionEvent]]

  protected val historyEventsLock = new AnyRef

  // followerId to eventsIds
  private val askFromFollower = new mutable.HashMap[Int, HashSet[(Int, Int)]]

  // follower ask leader for some events
  private val askList = new HashSet[(Int, Int)]

  protected val askListLock = new AnyRef

  private val rmHost = sc.getConf.get("spark.local.namenode1")

  private val id: Int = sc.getConf.getInt("spark.clusterId." + rmHost, -1)

  private var leaderId: Int = -1

  private var isLeader = false

  private var alivePartners = new mutable.HashSet[Int]

  @volatile private var syncInfo: SyncInfo = _

  protected val syncInfoLock = new Object()

  private var recoverInfo: RecoverInfo = new RecoverInfo()

  private val jobResults = new HashMap[Int, HashMap[Int, Any]]

  private val partnersStage = new HashMap[Int, (Int, Int)]

  private val stagesReadyForEvents = new mutable.HashSet[Int]

  // partnersStage
  protected val stageLock = new AnyRef

  protected val submitTaskLock = new AnyRef

  protected val leaderChangeLock = new AnyRef

  private var stageIdx = 1

  private var betweenStage = true

  private var stageOnSubmiting = -1

  // if it's in the middle of recovering
  private var recoveringJob = false

  private var recoveringStage = false

  // val completionEventPoll = new HashSet[CompletionEvent]

  // private val completionEventHandled = new HashSet[CompletionEvent]

  private val taskDistributor =
    new TaskDistributor(sc.getConf.get("spark.remote.taskDistributeMode", "random"))

  private val partnerNum = sc.getConf.getInt("spark.remote.allamnum", 1)

  private val groupName = sc.getConf.get("spark.remote.appname")

  private val recoverPartners = new mutable.HashSet[Int]

  protected val recoverLock = new AnyRef

  private val driverUrl = RpcEndpointAddress(
    sc.getConf.get("spark.driver.host"),
    sc.getConf.get("spark.driver.port").toInt,
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
  logInfo(s"driverUrl: ${driverUrl}")

  private val zkClient = sc.getZKClient

  protected val originalsLock = new AnyRef


  logInfo("DAGScheduler started")

  // send remote events
  private val sendRemoteEvents =
  ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-sendRemoteEvents")

  private val sendEventInterval = sc.getConf.getInt("spark.sendinterval.ms", 6000)

  def startSendRemoteEvents(): Unit = {

    val sendEvents = new Runnable() {
      override def run(): Unit = {
        try {
          if (zkClient.getLeader() != leaderId && leaderId != -1) {
            onLeaderChanged()
          }

          if (isLeader) {
            /*
            syncInfoLock.synchronized {
              // syncInfo.addTestNum(testNum)
              testNum += 1
              logInfo(s"testNum: ${testMap}")
            }
            */
            checkRecover()

            val newAlivePartners = zkClient.getPartners()
            if (newAlivePartners != alivePartners) {
              val oldAlivePartners = alivePartners
              alivePartners = newAlivePartners
              logInfo(s"alivePartners: ${alivePartners.toString()}")
              onFollowerStateChanged(newAlivePartners, oldAlivePartners)
            }
            sendRemoteEventsToFollowers()
          } else {
            val sInfo = getSyncInfo()
            // logInfo(s"testNum: ${sInfo.forTest}")
            // submitWaitingStages()
            // AccumulatorContext.setOriginals(getSyncInfo().originals)

            eventsToHandleLock.synchronized {
              val eh = eventsToHandle
              if (!eh.isEmpty) {
                eh.foreach { e =>
                  if (stagesReadyForEvents.contains(e.task.stageId)) {
                    handleTaskCompletion(e, true)
                    eventsToHandle -= e
                  }
                }
              }
            }

            sendRemoteEventsToLeader()
          }
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    logInfo("startSendRemoteEvents")
    sendRemoteEvents.scheduleWithFixedDelay(sendEvents, 0, sendEventInterval, TimeUnit.MILLISECONDS)

  }

  def onLeaderChanged(): Unit = leaderChangeLock.synchronized{
    logInfo("in leaderChangeLock onleaderchange")
    getSyncInfo(true)
    val oldLeader = leaderId
    leaderId = zkClient.getLeader()
    isLeader = zkClient.isLeader()
    logInfo(s"leader changed, old leader: ${oldLeader}, " + s"new leader: ${leaderId}")
    if (isLeader) {
      logInfo(s"become leader, id: ${id}")

      alivePartners.synchronized {
        // TODO: zkClient.getPartners() has bug
        alivePartners = zkClient.getPartners()
        logInfo(s"alivePartners: $alivePartners")
        if (alivePartners.contains(oldLeader)) {
          alivePartners.remove(oldLeader)
          logInfo(s"alivePartners: $alivePartners")
        }
      }
      syncInfoLock.synchronized {
        syncInfo.changeLeader(leaderId, driverUrl)
        zkClient.updateSyncInfo(syncInfo)
      }

      val leaderSet = new HashSet[Int]
      leaderSet.add(oldLeader)
      handlePartnersFailed(leaderSet, true)
    } else {
      var sInfo = getSyncInfo()
      var lId = sInfo.getLeaderInfo()._1
      while (leaderId != lId) {
        logInfo(s"new leader has not update syncInfo yet, old leader: ${lId}")
        while (!zkClient.isSyncInfoUpdate()) {
          logInfo("waiting for new syncInfo")
          Thread.sleep(1000)
        }
        sInfo = getSyncInfo()
        lId = sInfo.getLeaderInfo()._1
      }
      logInfo(s"registerAsFollower to new leader: ${lId}")
      registerAsFollower(lId, sInfo.getLeaderInfo()._2, sInfo.getLeaderInfo()._3)
    }
    logInfo("out leaderChangeLock onleaderchange")
  }

  def onFollowerStateChanged(newAlivePartners: mutable.HashSet[Int],
                             oldAlivePartners: mutable.HashSet[Int]): Unit = {
    logInfo("follower state changed, " +
      s"oldAlivePartners: $oldAlivePartners, newAlivePartners: $newAlivePartners")
    if(!newAlivePartners.diff(oldAlivePartners).isEmpty) {
      // TODO: new partner back to alive
      logInfo("partners back to alive")
      val recoverPartner = newAlivePartners.diff(oldAlivePartners)
      var unHandlePartners = recoverPartner
      syncInfoLock.synchronized {
        recoverPartner.foreach {pid =>
          if (syncInfo.partners(pid).appMasterState != ApplicationMasterState.FAILED) {
            unHandlePartners -= pid
          } else {
            syncInfo.setFollowerState(pid, ApplicationMasterState.RECOVERING)
          }
        }
        if (!unHandlePartners.isEmpty) {
          zkClient.updateSyncInfo(syncInfo)
        }
      }
      if (!unHandlePartners.isEmpty) {
        handlePartnersRecover(unHandlePartners)
      }

    } else if (!oldAlivePartners.diff(newAlivePartners).isEmpty) {
      val failedPartners = oldAlivePartners.diff(newAlivePartners)
      var unHandlePartners = failedPartners
      logInfo(s"partner failed: ${failedPartners.toString()}")
      syncInfoLock.synchronized {
        failedPartners.foreach{ pid =>
          if (syncInfo.partners(pid).appMasterRole == ApplicationMasterRole.LEADER) {
            unHandlePartners -= pid
          } else if (syncInfo.partners(pid).appMasterState == ApplicationMasterState.FAILED) {
            unHandlePartners -= pid
          } else {
            syncInfo.setFollowerState(pid, ApplicationMasterState.FAILED)
            logInfo(s"set follower ${pid} as failed")
          }
        }
        if (!unHandlePartners.isEmpty) {
          zkClient.updateSyncInfo(syncInfo)
        }
      }
      if (!unHandlePartners.isEmpty) {
        handlePartnersFailed(failedPartners, false)
      }
    }
    logInfo("follower state changed out")
  }

  def onSchedulerBackendStarted(endpointRef: RpcEndpointRef): Unit = {
    // 1. register To ZK
    // 2. Leader get follower Info and send syncInfo
    // 3. follower get syncInfo and register to leader
    // 4. startSendRemoteEvents()
    logInfo(s"register to zk, id: ${id}" + s" ,group: ${groupName}")
    syncInfo = new SyncInfo()
    zkClient.register(id, groupName)
    isLeader = zkClient.isLeader()
    if(isLeader) {
      logInfo("elected as leader initially")
      val partners = new HashMap[Int, ClusterInfo]

      var registeredPartners = zkClient.getPartners()
      while (registeredPartners.size < partnerNum) {
        logInfo(s"total partners number: ${partnerNum}"
          + s" registered partners number: ${registeredPartners.size}"
          + s" registered partners: ${registeredPartners.toString()}")
        Thread.sleep(1000)
        registeredPartners = zkClient.getPartners()
      }
      logInfo(s"all registered partners: ${registeredPartners.toString()}")

      val nodesOfClusters = XML.load("__spark_conf__/clustersInfo.xml")
      val nodesMap = (HashMap[Int, (String, mutable.HashSet[String])]()
        /: (nodesOfClusters \ "cluster")) {
        (map, cluster) =>
          val id = (cluster \ "@id").toString.toInt
          val master = (cluster \ "master").text.toString
          val slaves = (cluster \ "slaves").text.toString
          val nodesSet = slaves.split(",")
          val scalaSet: mutable.HashSet[String] = mutable.HashSet(nodesSet: _*)
          scalaSet += master
          map(id) = (master, scalaSet)
          map
      }
      logInfo(s"clusterNodes: $nodesMap")
      registeredPartners.foreach { fid =>
        if (fid == id) {
          val selfInfo = new ClusterInfo()
          selfInfo.setClusterId(id)
          // selfInfo.setClusterName(sc.getConf.get("spark.clustername.id" + id.toString))
          selfInfo.setClusterName(nodesMap(fid)._1)
          selfInfo.setNodes(nodesMap(fid)._2)
          selfInfo.setDriverUrl(driverUrl)
          selfInfo.setEndpointRef(endpointRef)
          selfInfo.setAppMasterRole(ApplicationMasterRole.LEADER)
          selfInfo.setAppMasterState(ApplicationMasterState.RUNNING)
          partners.put(id, selfInfo)
        } else {
          val info = new ClusterInfo()
          info.setClusterId(fid)
          // info.setClusterName(sc.getConf.get("spark.clustername.id" + fid.toString))
          info.setClusterName(nodesMap(fid)._1)
          info.setNodes(nodesMap(fid)._2)
          info.setAppMasterRole(ApplicationMasterRole.FOLLOWER)
          info.setAppMasterState(ApplicationMasterState.RUNNING)
          partners.put(fid, info)
        }
      }
      alivePartners = registeredPartners
      syncInfoLock.synchronized{
        syncInfo.setGroup(groupName)
        syncInfo.setPartners(partners)
        // syncInfo.setOriginals(AccumulatorContext.getRegisterSet())
        zkClient.updateSyncInfo(syncInfo)
      }
      logInfo("update syncInfo first time")
      updateSyncEpoch(1)
    } else {
      leaderId = zkClient.getLeader()
      logInfo(s"elected as follower initially, leaderId ${leaderId}")

      /*
      while (!zkClient.isSyncInfoUpdate()) {
        Thread.sleep(1000)
      }
      syncInfo = zkClient.getSyncInfo()
      */

      while (syncInfo.groupName != groupName) {
        logInfo(s"groupName not right, syncInfo: ${syncInfo.groupName}, local: ${groupName}")
        Thread.sleep(1000)
        getSyncInfo()
      }
      logInfo(s"syncInfo.groupName: ${syncInfo.groupName}")
      val (lid, leaderEndpoint, leaderUrl) = syncInfo.getLeaderInfo()
      logInfo(s"registerAsFollower to leader: ${lid}, leaderUrl: ${leaderUrl}")
      // AccumulatorContext.setOriginals(syncInfo.originals)
      registerAsFollower(lid, leaderEndpoint, leaderUrl)
      updateSyncEpoch(1)
    }
    startSendRemoteEvents()
  }

  def getSyncInfo(leaderChange: Boolean = false): SyncInfo = {
    syncInfoLock.synchronized {
      if (leaderChange) {
        if (zkClient.isSyncInfoUpdate()) {
          syncInfo = zkClient.getSyncInfo()
        } else {
          Thread.sleep(700)
          if (zkClient.isSyncInfoUpdate()) {
            syncInfo = zkClient.getSyncInfo()
          }
        }
        return syncInfo
      }

    if (!isLeader && zkClient.isSyncInfoUpdate()) {
        syncInfo = zkClient.getSyncInfo()
      }
      val sInfo = syncInfo
      sInfo
    }
  }

  def checkLeader(): Unit = leaderChangeLock.synchronized{
    // logInfo("in leaderChangeLock checkLeader")
    isLeader = zkClient.isLeader()
    if (isLeader && zkClient.getLeader() != leaderId && leaderId != -1) {
      getSyncInfo(true)
    }
    // logInfo("out leaderChangeLock checkLeader")
  }

  def onJobStarted(jobId: Int): Unit = {
    logInfo(s"onJobStarted, jobId: ${jobId}")
    zkClient.updateStageInfo(jobId, -1)

    var alright = false
    while (!alright) {
      var wait = false
      val ps = zkClient.getStageInfo()
      val sInfo = getSyncInfo()
      ps.foreach {case(pid, (jId, sId)) =>
        if (jobId > jId
          && sInfo.partners(pid).appMasterState == ApplicationMasterState.RUNNING) {
          wait = true
          logInfo(s"partner ${pid} is not ready yet, jobId: ${jId}, stageId: ${sId}")
        }
      }
      if (wait) {
        Thread.sleep(1000)
      } else {
        alright = true
      }
    }

    if (false) {
      logInfo("clear events cache")
      eventsHandled.clear()
      eventsFromLeader.clear()
      localLock.synchronized{
        eventsFromLocalOnLeader.clear()
        eventsFromLocalOnFollower.clear()
      }
      followerLock.synchronized{eventsFromFollower.clear()}
      toLeaderLock.synchronized{eventsToSendToLeader.clear()}
      historyEventsLock.synchronized{historyEventsByEpoch.clear()}
      successLock.synchronized{eventsSuccess.clear()}
    }


    logInfo(s"job $jobId shall start")
  }

  def onStageStarted(jobId: Int, stageId: Int): Unit = {
    logInfo(s"onStageStarted, jobId: ${jobId}, stageId: ${stageId}")
    betweenStage = true
    zkClient.updateStageInfo(jobId, stageId)
    return

    if (true) {
      zkClient.updateStageInfo(jobId, stageId)

      // wait for follower
      var allright = false
      while (!allright) {
        var wait = false
        val ps = zkClient.getStageInfo()
        ps.foreach {case(pid, (jId, sId)) =>
          if ((jobId, stageId) != (jId, sId)) {
            wait = true
            logInfo(s"partner ${pid} is not ready yet, jobId: ${jId}, stageId: ${sId}")
          }
        }
        if (wait) {
          Thread.sleep(1000)
        } else {
          allright = true
        }

      }

    } else {
      // send info to leader
      // taskScheduler.onStageStarted(id, jobId, stageId)
      zkClient.updateStageInfo(jobId, stageId)

      // wait

    }

    /*
    logInfo("clear events cache")
    eventsHandled.clear()
    eventsFromLeader.clear()
    localLock.synchronized{
      eventsFromLocalOnLeader.clear()
      eventsFromLocalOnFollower.clear()
    }
    followerLock.synchronized{eventsFromFollower.clear()}
    toLeaderLock.synchronized{eventsToSendToLeader.clear()}
    historyEventsLock.synchronized{historyEventsByEpoch.clear()}
    */

    logInfo(s"stage $stageId shall start")
  }

  /** Called to get current epoch number. */
  def getSyncEpoch: Long = {
    syncEpochLock.synchronized {
      return syncEpoch
    }
  }

  def updateSyncEpoch(newEpoch: Long) {
    syncEpochLock.synchronized {
      if (newEpoch > syncEpoch) {
        logInfo(s"Updating epoch to ${newEpoch}")
        syncEpoch = newEpoch
        // TODO
      }
    }
  }

  def addEventsToHistory(epoch: Long, events: HashSet[CompletionEvent]): Unit = {
    historyEventsLock.synchronized {
      historyEventsByEpoch.getOrElseUpdate(epoch, new HashSet[CompletionEvent]()) ++= events
    }
  }

  def getEventsToHistory(): HashMap[Long, HashSet[CompletionEvent]] = {
    historyEventsLock.synchronized {
      historyEventsByEpoch.clone()
    }
  }

  def addEventToSendOnFollower(event: CompletionEvent): Unit = {
    toLeaderLock.synchronized {
      eventsToSendToLeader += event
    }
  }

  def getEventsToSendToLeader(): HashSet[CompletionEvent] = {
    var events = new HashSet[CompletionEvent]
    toLeaderLock.synchronized {
      events = eventsToSendToLeader.clone()
      eventsToSendToLeader.clear()
    }
    events
  }


  def getEventsFromFollower(getAndClear: Boolean = true): HashMap[Int, HashSet[CompletionEvent]] = {

    var events = new mutable.HashMap[Int, HashSet[CompletionEvent]]
    followerLock.synchronized {
      events = eventsFromFollower.clone()
      if (getAndClear) {
        eventsFromFollower.clear()
      }
    }
    events
  }

  def getEventsFromLocal(getAndClear: Boolean = true): HashSet[CompletionEvent] = {

    var events = new HashSet[CompletionEvent]
    localLock.synchronized {
      if (isLeader) {
        events = eventsFromLocalOnLeader.clone()
        if (getAndClear) {
          eventsFromLocalOnLeader.clear()
        }
      } else {
        events = eventsFromLocalOnFollower.clone()
        if (getAndClear) {
          eventsFromLocalOnFollower.clear()
        }
      }
    }
    events
  }

  def addEventsFromLocal(event: CompletionEvent): Unit = {
    localLock.synchronized {
      if (isLeader) {
        eventsFromLocalOnLeader += event
      } else {
        eventsFromLocalOnFollower += event
      }
    }
  }

  // return map[followerId, eventSet]
  def getEventsToSendOnLeader(getAndClear: Boolean): HashMap[Int, HashSet[CompletionEvent]] = {
    val followerEvents = getEventsFromFollower(getAndClear)
    val localEvents = getEventsFromLocal(getAndClear)

    var historyEventsById = new HashMap[(Int, Int), CompletionEvent]
    getEventsToHistory().foreach { case(ep, es) =>
      historyEventsById ++= es.map {e => ((e.task.stageId, e.task.partitionId), e)}.toMap
    }

    val followerEventsSet = new HashSet[CompletionEvent]
    followerEvents.foreach{ case(fid, e) =>
      followerEventsSet ++= e
    }

    var asks = new mutable.HashMap[Int, HashSet[(Int, Int)]]
    askListLock.synchronized {
      asks = askFromFollower.clone()
      askFromFollower.clear()
    }
    val eventsToSend = new HashMap[Int, HashSet[CompletionEvent]]
    // due with followers have send events this epoch
    followerEvents.foreach{ case(fid, es) =>
      val askResponse = new mutable.HashSet[CompletionEvent]
      if (asks.isDefinedAt(fid)) {
        asks(fid).foreach { eid =>
          if (historyEventsById.isDefinedAt(eid)) {
            askResponse += historyEventsById(eid)
          }
        }
      }
      if (!askResponse.isEmpty) {
        logInfo(s"response to follower ${fid}: ${askResponse.map(e =>
          (e.task.stageId, e.task.partitionId))}")
      }
      eventsToSend.put(fid, followerEventsSet.diff(es) ++ localEvents ++ askResponse)
    }

    // due with followers have not send events this epoch
    val allEventsThisEpoch = followerEventsSet ++ localEvents
    getSyncInfo().partners.foreach{ case(pid, info) =>
        if (!eventsToSend.isDefinedAt(pid) && pid != id
          && info.appMasterState != ApplicationMasterState.FAILED) {
          val askResponse = new mutable.HashSet[CompletionEvent]
          if (asks.isDefinedAt(pid)) {
            asks(pid).foreach { eid =>
              if (historyEventsById.isDefinedAt(eid)) {
                askResponse += historyEventsById(eid)
              }
            }
          }
          if (!askResponse.isEmpty) {
            logInfo(s"response to follower ${pid}: ${askResponse.map(e =>
              (e.task.stageId, e.task.partitionId))}")
          }
          eventsToSend.put(pid, allEventsThisEpoch ++ askResponse)
        }
    }

    // TODO: what if leader receive followers' event before they get new epoch
    if(!allEventsThisEpoch.isEmpty) {
      addEventsToHistory(ceEpoch, allEventsThisEpoch)
    }

    eventsToSend
  }

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }


  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      taskInfo: TaskInfo): Unit = {
    /*
    var resultMaybeWithAppId = result
    reason match {
      case Success =>
        task match {
          case smt: ShuffleMapTask =>
            val status = result.asInstanceOf[MapStatus]
            status.location.appId = sc.getConf.getAppId
            resultMaybeWithAppId = status
        }
      case _ =>
    }
    */

    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Get or create a shuffle map stage for the given shuffle dependency's map side.
   */
  private def getShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        // We are going to register ancestor shuffle dependencies
        getAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          if (!shuffleToMapStage.contains(dep.shuffleId)) {
            shuffleToMapStage(dep.shuffleId) = newOrUsedShuffleStage(dep, firstJobId)
          }
        }
        // Then register current shuffleDep
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }

  /**
   * Helper function to eliminate some code re-use when creating new stages.
   */
  private def getParentStagesAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    val parentStages = getParentStages(rdd, firstJobId)
    val id = nextStageId.getAndIncrement()
    (parentStages, id)
  }

  /**
   * Create a ShuffleMapStage as part of the (re)-creation of a shuffle map stage in
   * newOrUsedShuffleStage.  The stage will be associated with the provided firstJobId.
   * Production of shuffle map stages should always use newOrUsedShuffleStage, not
   * newShuffleMapStage directly.
   */
  private def newShuffleMapStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int,
      callSite: CallSite): ShuffleMapStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, firstJobId)
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      firstJobId, callSite, shuffleDep)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(firstJobId, stage)
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def newResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)
    val stage = new ResultStage(id, rdd, func, partitions, parentStages, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Create a shuffle map Stage for the given RDD.  The stage will also be associated with the
   * provided firstJobId.  If a stage for the shuffleId existed previously so that the shuffleId is
   * present in the MapOutputTracker, then the number and location of available outputs are
   * recovered from the MapOutputTracker
   */
  private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId, rdd.creationSite)
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, firstJobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }
            case _ =>
          }
          waitingForVisit.push(dep.rdd)
        }
      }
    }

    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                  waitingStagesWontClear -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  def submitJobOnLeader[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = leaderChangeLock.synchronized{
    // logInfo("in leaderChangeLock submitJobOnLeader")
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    lastJobId = jobId
    logInfo(s"submitJobOnLeader, jobId: ${jobId}")

    // sync point (rdd, partitions, jobId)
    syncInfoLock.synchronized {
      syncInfo.setJobId(jobId)
      syncInfo.setRDD(rdd)
      syncInfo.setPartitions(partitions)
      zkClient.updateSyncInfo(syncInfo)
    }

    if (partitions.size == 0) {
      logInfo("this job has no tasks to run")
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    // logInfo("out leaderChangeLock submitJobOnLeader")
    waiter
  }

  def submitJobOnFollower[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {

    // sync Point (rdd, partitions, jobId)
    // check if isLeader
    // if true, call submitJobOnLeader()
    checkLeader()
    if (isLeader) {
      return submitJobOnLeader(rdd, func, partitions, callSite, resultHandler, properties)
    }

    var sInfo = getSyncInfo()
    logInfo(s"lastJobId: ${lastJobId}, syncJobId: ${sInfo.jobId}, "
      + s"appState: ${sInfo.partners(id).appMasterState}")
    while (sInfo.jobId == -1 || lastJobId + 1 != sInfo.jobId
      && sInfo.partners(id).appMasterState != ApplicationMasterState.RECOVERING) {
      checkLeader()
      if (isLeader) {
        return submitJobOnLeader(rdd, func, partitions, callSite, resultHandler, properties)
      }
      logInfo("wait leader to update syncInfo, "
        + s"lastJobId: ${lastJobId}, syncJobId: ${sInfo.jobId}")
      Thread.sleep(1000)
      sInfo = getSyncInfo()
    }

    logInfo(s"lastJobId: ${lastJobId}, syncJobId: ${sInfo.jobId}, "
      + s"appState: ${sInfo.partners(id).appMasterState}")

    val rddClass = sInfo.rdd.getClass
    logInfo(s"rddClass: ${sInfo.rdd.getClass}")
    // val rddFromLeader: RDD[T] = sInfo.rdd.asInstanceOf[RDD[T]]
    val rddFromLeader = rdd
    var partitionsFromLeader = sInfo.partitions
    if (sInfo.partners(id).appMasterState != ApplicationMasterState.RUNNING) {
      partitionsFromLeader = partitions
    }
    // val jobIdFromLeader = syncInfo.jobId

    if (rddFromLeader == null) {
      logInfo("rddFromLeader is null")
    }
    if (partitions.isEmpty) {
      logInfo("partition is empty")
    }

    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rddFromLeader.partitions.length
    partitionsFromLeader.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    lastJobId = jobId

    if (partitionsFromLeader.size == 0) {
      logInfo("this job has no tasks to run")
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitionsFromLeader.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitionsFromLeader.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rddFromLeader, func2, partitionsFromLeader.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  /**
    * Recover this application's jobs with the recover data from Leader.
    * recover data:
    * Success job table
    *   job1 => result data(rt.outputId => event.result)
    *   job2 => result data
    *   ...
    *
    */
  def recoverJob(job: ActiveJob): Unit = {
    // return current local job's result to JobWaiter directly.
    logInfo("recoveringJob(" + job + ")")

    recoverInfo = getRecoverInfoFromLeader()
    logInfo("get recoverInfo")
    recoverInfo.jobResults.get(job.jobId).get.foreach{ case(tid, rt) =>
      job.listener.taskSucceeded(tid, rt)
    }
  }

  /**
    * Recover Running job's stage(one stage at once) with the recover data from Leader.
    * recover data:
    * 1. Running stage id
    * 2. Latest completed stage's snapshot of MapOutputTracker info(mapStatuses & epoch: simple way)
    *     or each stage's results(not simple way)
    * 3. Running stage's completed tasks' results
    *
    * Return true if success
    */
  def recoverStage(stage: Stage): Unit = {
    // 1. if current local stageId < remote stageId
    //      submit a empty TaskSet, markStageAsFinished;
    // 2. if current local stageId == remote stageId
    //      (1) recover MapOutputTracker's info
    //      (2) handle completed tasks' results;

    logInfo("recoveringStage(" + stage + ")")
    recoverInfo = getRecoverInfoFromLeader()

    if (stage.id == recoverInfo.runningStageId - 1) {
      logInfo("recover MapStatuses(" + stage + ")")
      mapOutputTracker.recoverMapStatuses(recoverInfo.mapStatuses)
    }
    stage.pendingPartitions.clear()
    runningStages += stage

    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }

    markStageAsFinished(stage, None)
  }


  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @throws Exception when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    // val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)

    while (getSyncEpoch == 0) {
      // make sure the first sync has been made
      // first sync should be done at or before this point, to make sure who is Leader.
      logInfo("first sync has not been made yet")
      Thread.sleep(1000)
    }
    onJobStarted(lastJobId + 1)
    // completionEventPoll.clear()
    // completionEventHandled.clear()

    checkLeader()
    val waiter = isLeader match {
      case true =>
        submitJobOnLeader(rdd, func, partitions, callSite, resultHandler, properties)
      case false =>
        submitJobOnFollower(rdd, func, partitions, callSite, resultHandler, properties)
    }

    // TODO: (Followers) check if local jobId == remote jobId
    // TODO: if not, call recoverApplication()

    // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
    // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
    // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
    // safe to pass in null here. For more detail, see SPARK-13747.
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator [[ApproximateEvaluator]] to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  /**
   * Cancel all jobs in the given job group ID.
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
    submitWaitingStages()
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Ordinarily run on every iteration of the event loop.
   */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    /*
    for (stage <- waitingStagesCopy.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
    */
    for (stage <- waitingStagesCopy.sortBy(_.id)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = newResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))

    // sync first, check if local jobId == remote jobId
    // if not, don't submit stage, just continue
    checkLeader()
    val leaderJobId = getSyncInfo().jobId
    if(!isLeader && jobId < leaderJobId && false) {
      logInfo(s"recover job, leader job id: ${leaderJobId}, local job id: ${jobId}")
      recoverJob(job)
    } else {
      recoverInfo.setJobId(jobId)
      logInfo(s"submit job ${jobId} finalStage")
      submitStage(finalStage)

      submitWaitingStages()
    }
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }

    submitWaitingStages()
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage): Unit = leaderChangeLock.synchronized{
    // logInfo("in leaderChangeLock submit stage")
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          // submitMissingTasks(stage, jobId.get)

          // to make sure the Leader is still alive
          checkLeader()
          if (isLeader) {
            submitTaskLock.synchronized {
              onStageStarted(jobId.get, stage.id)
              syncInfoLock.synchronized {
                syncInfo.setStageId(stage.id)
                syncInfo.addStage(stageIdx, stage.id)
                // logInfo(s"stageSeq: ${syncInfo.stageSeq}")
                zkClient.updateSyncInfo(syncInfo)
                stageIdx += 1
              }
              submitMissingTasksOnLeader(stage, jobId.get)
            }
          } else {

            /*
            if (jobId.get < sInfo.jobId) {
              recoverJob(jobIdToActiveJob(jobId.get))
              return
            }
            if (stage.id < sInfo.stageId) {
              recoverStage(stage)
              return
            }
            */
            // TODO: follow leader step

            /*
            val sInfo = getSyncInfo()
            logInfo(s"stageIdx: $stageIdx")
            if (!sInfo.stageSeq.isDefinedAt(stageIdx)
              || sInfo.stageSeq(stageIdx) != stage.id) {
              logInfo(s"stageSeq: ${sInfo.stageSeq}")
              waitingStages += stage
              return
            }
            */

            submitTaskLock.synchronized {
              stageIdx += 1
              onStageStarted(jobId.get, stage.id)
              submitMissingTasksOnFollower(stage, jobId.get)
            }
          }
          stagesReadyForEvents += stage.id
          recoverInfo.setStageId(stage.id)
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStagesWontClear += stage
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
    // logInfo("out leaderChangeLock submit stage")
  }

  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingPartitions.clear()

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.activeJob.get
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.latestInfo.taskMetrics, properties)
          }

        case stage: ResultStage =>
          val job = stage.activeJob.get
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingPartitions ++= tasks.map(_.partitionId)
      logDebug("New pending partitions: " + stage.pendingPartitions)
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }


  private def submitMissingTasksOnLeader(stage: Stage, jobId: Int) {
    logInfo("submitMissingTasksOnLeader(" + stage + ")")
    logInfo(s"pendingPartitions: ${stage.pendingPartitions.toString()}")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingPartitions.clear()

    // sync point, distribute Leadership and subPartitions to other cluster
    // jobId, stageId(attempId), subPartitions1, subPartitions2, ...)
    // use partitionsToCompute to makeNewStageAttempt()
    // use subPartition to get taskIdToLocations
    // stage.pendingPartitions should be global

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
    logInfo(s"allPartitions: ${partitionsToCompute}")

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.activeJob.get
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    var partners: HashMap[Int, ClusterInfo] = HashMap.empty
    syncInfoLock.synchronized {
      if (!getSyncInfo().partners(id).subPartitions.isDefinedAt(stage.id)) {
        partners = taskDistributor.distributeTask(stage.id, partitionsToCompute,
          taskIdToLocations, syncInfo.partners)
        syncInfo.setPartners(partners)
        syncInfo.setSubPartFlag(jobId, stage.id)
        // syncInfo.setOriginals(AccumulatorContext.getRegisterSet())
        zkClient.updateSyncInfo(syncInfo)
      } else {
        partners = syncInfo.partners
      }
    }
    val partitionsComputeLocal = partners(id).subPartitions(stage.id)
    val taskIdToLocationsLocal = partitionsComputeLocal.map {id =>
      (id, taskIdToLocations(id))}.toMap

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocationsLocal.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    val taskLocal: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsComputeLocal.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.latestInfo.taskMetrics, properties)
          }

        case stage: ResultStage =>
          val job = stage.activeJob.get
          partitionsComputeLocal.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    /*
    val partners = taskDistributor.distributeTaskByRandom(stage.id, tasks, partitionsToCompute,
      getSyncInfo().partners)

    syncInfoLock.synchronized{
      syncInfo.setPartners(partners)
      syncInfo.setSubPartFlag(jobId, stage.id)
      // syncInfo.setOriginals(AccumulatorContext.getRegisterSet())
      zkClient.updateSyncInfo(syncInfo)
    }

    logInfo("distribute tasks")
    partners.foreach { case(pid, info) =>
      logInfo(s"partner ${pid} handle tasks ${info.subPartitions(stage.id).toString()}")
    }
    betweenStage = false
    */

    // val taskLocal = partners(id).subTasks(stage.id)

    if (taskLocal.size > 0) {
      logInfo("Submitting " + taskLocal.size + " missing tasks from "
        + stage + " (" + stage.rdd + ")")
      // stage.pendingPartitions ++= tasks.map(_.partitionId)
      stage.pendingPartitions ++= partitionsToCompute.toSet
      logDebug("New pending partitions: " + stage.pendingPartitions)
      taskScheduler.submitTasks(new TaskSet(
        taskLocal.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      logInfo("no tasks to run")
      if (partitionsToCompute.size == 0) {
        logInfo(s"markStageAsFinished: ${stage.id}")
        markStageAsFinished(stage, None)
      }
      stage.pendingPartitions ++= partitionsToCompute.toSet
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }


  private def submitMissingTasksOnFollower(stage: Stage, jobId: Int) {
    logInfo("submitMissingTasksOnFollower(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingPartitions.clear()

    // sync point
    // 1. check if isLeader, if true, call submitMissingTasksOnLeader()

    // 4. after recover, get subPartitions from Leader
    //     (jobId, stageId(attempId), subPartitions1, subPartitions2, ...)
    //     use partitionsToCompute to makeNewStageAttempt()
    //     use subPartition to get taskIdToLocations
    //     stage.pendingPartitions should be global


    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
    logInfo(s"allPartitions: ${partitionsToCompute}")
    runningStages += stage

    var sInfo = getSyncInfo()
    while (!sInfo.partners(id).subPartitions.isDefinedAt(stage.id)) {
      // wait and get syncInfo
      logInfo("wait and get syncInfo when submit tasks")
      logInfo(s"local: jobId $jobId, stageId ${stage.id}")
      logInfo(s"sync:  jobId ${sInfo.jobId}, stageId ${sInfo.stageId}")
      logInfo(s"flag:  jobId ${sInfo.subPartFlag._1}, stageId ${sInfo.subPartFlag._2}")
      Thread.sleep(1000)
      sInfo = getSyncInfo()
      checkLeader()
      if (isLeader) {
        onStageStarted(jobId, stage.id)
        submitMissingTasksOnLeader(stage, jobId)
        return
      }
    }


    logInfo("betweenStage: false")
    val partitionsComputeLocal = sInfo.partners(id).subPartitions(stage.id)
    logInfo(s"receive subPartitions: ${partitionsComputeLocal}")
    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties


    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsComputeLocal.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.activeJob.get
          partitionsComputeLocal.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }


    val taskLocal: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsComputeLocal.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.latestInfo.taskMetrics, properties)
          }

        case stage: ResultStage =>
          val job = stage.activeJob.get
          partitionsComputeLocal.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    /*
    val taskLocal = tasks.filter{t => partitionsComputeLocal.isDefinedAt(t.partitionId)}
    originalsLock.synchronized {
      AccumulatorContext.clear()
      getSyncInfo().originalsRegisterSet.foreach { a =>
        AccumulatorContext.register(a)
      }
    }
    */

    if (taskLocal.size > 0) {
      logInfo("Submitting " + taskLocal.size + " missing tasks from "
        + stage + " (" + stage.rdd + ")")
      // stage.pendingPartitions ++= tasks.map(_.partitionId)
      stage.pendingPartitions ++= partitionsToCompute.toSet
      logDebug("New pending partitions: " + stage.pendingPartitions)
      taskScheduler.submitTasks(new TaskSet(
        taskLocal.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      logInfo("no tasks to run")
      if (partitionsToCompute.size == 0) {
        logInfo(s"markStageAsFinished: ${stage.id}")
        markStageAsFinished(stage, None)
      }
      stage.pendingPartitions ++= partitionsToCompute.toSet
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }

  private def submitMissingTasksAdditional(stage: Stage, jobId: Int, partitions: Seq[Int]): Unit = {
    // called when Leader or Follower down, submit it's failed tasks
    // in TaskScheduler, only one TaskSetManager(include one TaskSet) are allowed
    // for a stage attemp, so we need to maintain the TaskSets(etc TaskSetManagers in TaskScheduler)
    // which are submitted after the first one. And every TaskSetManager should have
    // an unique name, so the schedulingMode(taskSetScheduler) can schedule them.
    // Add a submitTasksAdditional() to due with these new TaskSets.

    logInfo("submitMissingTasksAdditional(" + stage + "), "
      + s"partitions: ${partitions.sorted}")
    // Get our pending tasks and remember them in our pendingTasks entry

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = partitions

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties

    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.

    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.activeJob.get
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        logInfo("Additional Task creation failed")
        // listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        return
    }

    // TODO: listenerBus.post(SparkListenerTaskSetSubmitted)


    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        // abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        // runningStages -= stage

        logInfo("Additional Task not serializable")

        // Abort execution
        return
      case NonFatal(e) =>
        // abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        // runningStages -= stage

        logInfo("Additional Task serialization failed")

        return
    }

    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.latestInfo.taskMetrics, properties)
          }

        case stage: ResultStage =>
          val job = stage.activeJob.get
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, stage.latestInfo.taskMetrics)
          }
      }
    } catch {
      case NonFatal(e) =>
        // abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        // runningStages -= stage
        logInfo("Additional Task creation failed")

        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " Additional tasks from "
        + stage + " (" + stage.rdd + ")")
      // stage.pendingPartitions ++= tasks.map(_.partitionId)
      // logDebug("New pending partitions: " + stage.pendingPartitions)
      if (!runningStages.contains(stage)) {
        return
      }
      val taskSetName = "TaskSetAdditional_" + jobId.toString + "_" + stage.id.toString
      taskScheduler.submitTasksAdditional(taskSetName,
        new TaskSet(tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      // stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run

      logInfo("Addition taskSet is empty")
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent, isRemote: Boolean): Unit = {
    if (isRemote) {
      return
    }
    /*
    if (!isLeader) {
      originalsLock.synchronized {
        AccumulatorContext.clear()
        getSyncInfo().originalsRegisterSet.foreach { a =>
          AccumulatorContext.register(a)
        }
      }
    }
    */
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.accumulables += acc.toInfo(Some(updates.value), Some(acc.value))
        }

        // logInfo(s"updateAccumulators: stage: ${task.stageId}, partition: ${task.partitionId}, " +
        //   s"updatesId: ${id}")
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for " +
          s"task ${task.partitionId}, ${task.stageId}", e)
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent, isRemote: Boolean) {

    if (eventsHandled.contains(event)) {
      logInfo("event already handled")
      return
    } else {
      eventsHandled.add(event)
    }

    /*
    successLock.synchronized {
      if (eventsSuccess.contains(event.task.partitionId)) {
        return
      }
      event.reason match {
        case Success =>
          eventsSuccess += event.task.partitionId
        case _ =>
      }
    }
    */

    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    listenerBus.post(SparkListenerTaskEnd(
       stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        stage.pendingPartitions -= task.partitionId
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event, isRemote)
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  var updateRecoverInfo = false
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                    updateRecoverInfo = true
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                    updateJobResults(job.jobId, rt.outputId, event.result, updateRecoverInfo)
                    recoverInfo.setMapStatuses(mapOutputTracker.getMapStatuses())
                    recoverInfo.setStageId(0)
                    // completionEventHandled.add(event)
                    // completionEventPoll.add(event)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event, isRemote)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (!isRemote && failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              clearCacheLocs()

              recoverInfo.setMapStatuses(mapOutputTracker.getMapStatuses())
              recoverInfo.setStageId(recoverInfo.runningStageId + 1)

              if (!shuffleStage.isAvailable) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
              }

              // Note: newly runnable stages will be submitted below when we submit waiting stages
            }
        }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingPartitions += task.partitionId

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId)) {
            abortStage(failedStage, s"$failedStage (${failedStage.name}) " +
              s"has failed the maximum allowable number of " +
              s"times: ${Stage.MAX_CONSECUTIVE_FETCH_FAILURES}. " +
              s"Most recent failure reason: ${failureMessage}", None)
          } else {
            if (failedStages.isEmpty) {
              // Don't schedule an event to resubmit failed stages if failed isn't empty, because
              // in that case the event will already have been scheduled.
              // TODO: Cancel running tasks in the stage
              logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
            failedStages += failedStage
            failedStages += mapStage
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Tasks failed with exceptions might still have accumulator updates.
        updateAccumulators(event, isRemote)

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | TaskKilled | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
    submitWaitingStages()
  }

  private[scheduler] def handleTaskCompletionFromLocal(event: CompletionEvent): Unit = {
    event.reason match {
      case Success =>
        addEventsFromLocal(event)
        checkLeader()
        // logInfo(s"TaskCompletionFromLocal, isLeader: ${isLeader}")
        if (!isLeader) {
          addEventToSendOnFollower(event)
        }

      case _ =>

    }
      handleTaskCompletion(event, false)

  }

  private[scheduler] def handleTaskCompletionFromRemote(followerId: Int,
                                                        events: HashSet[CompletionEvent]): Unit = {
    // called when receive new remote TaskCompletion event
    logInfo("handleTaskCompletionFromRemote")
    followerLock.synchronized {
      eventsFromFollower.getOrElseUpdate(followerId, new HashSet[CompletionEvent]()) ++= events
    }
    events.foreach{ e =>
      handleTaskCompletion(e, true)
    }
  }


  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
   * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
   * presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      filesLost: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.outputLocInMapOutputTrackerFormat(),
            changeEpoch = true)
        }
        if (shuffleToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    submitWaitingStages()
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
    submitWaitingStages()
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    betweenStage = true
    logInfo("betweenStage: true")
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo("%s (%s) failed in %s s".format(stage, stage.name, serviceTime))
    }

    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      job.listener.jobFailed(error)
      cleanupStateForJobAndIndependentStages(job)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def updateJobResults(jobId: Int, outputId: Int, result: Any, updateRecoverInfo: Boolean): Unit = {
    val results = jobResults.getOrElseUpdate(jobId, new mutable.HashMap[Int, Any]())
    results(outputId) = result

    if(updateRecoverInfo) {
      recoverInfo.setJobResults(jobResults)
    }
  }


  def checkRecover(): Unit = {
    recoverLock.synchronized {
      val rp = recoverPartners.clone()
      val ps = zkClient.getStageInfo()
      rp.foreach { pid =>
        if (ps(pid)._1 >= ps(id)._1) {
          syncInfoLock.synchronized {
            logInfo(s"partner ${pid} recover done")
            syncInfo.setFollowerState(pid, ApplicationMasterState.RUNNING)
            zkClient.updateSyncInfo(syncInfo)
            recoverPartners -= pid
          }
        }
      }
    }
  }

  def handlePartnersRecover(pids: HashSet[Int]): Unit = {
    logInfo(s"handlePartnersRecover")
    val checkRecover = new Runnable() {
      override def run(): Unit = {
        try {
          var alright = false
          while (!alright) {
            var wait = false
            val ps = zkClient.getStageInfo()
            val recoveringPartner = pids
            recoveringPartner.foreach { pid =>
              if (ps(pid)._1 < ps(id)._1) {
                wait = true
                logInfo(s"partner ${pid} is still recovering," +
                  s"jobId: ${ps(pid)._1}, stageId: ${ps(pid)._2}")
              } else {
                syncInfoLock.synchronized {
                  logInfo(s"partner ${pid} recover done")
                  syncInfo.setFollowerState(pid, ApplicationMasterState.RUNNING)
                  zkClient.updateSyncInfo(syncInfo)
                  pids -= pid
                }
              }
            }
            if (wait) {
              Thread.sleep(3000)
            } else {
              alright = true
            }
          }
        }
      }
    }
    // checkRecover.run()
    recoverLock.synchronized {
      recoverPartners ++= pids
    }
  }

  // called when partner(s) fail in Leader AM
  // 1. reSubmit failed tasks belong to failed partners
  // TODO: 2. reSubmit sAM to the clusters where failed partners belong to
  def handlePartnersFailed(pids: HashSet[Int], newLeader: Boolean): Unit = {
    // TODO: need to reWrite

    logInfo(s"handlePartnersFailed, betweenStage: ${betweenStage}, newLeader: ${newLeader}")

    val sInfo = getSyncInfo()
    val jobId = sInfo.jobId
    // val stage = stageIdToStage(sInfo.stageId)


    var allReceivedEventsIds: HashSet[Int] = HashSet.empty
    var allReceivedEvents: HashSet[CompletionEvent] = HashSet.empty

    if (newLeader) {
      eventsFromLeader.foreach {case(ep, es) =>
        allReceivedEvents ++= es
      }
      allReceivedEventsIds ++= allReceivedEvents.map {e => e.task.partitionId}

      // become leader, some events set should be moved
      addEventsToHistory(-1, allReceivedEvents)
      eventsFromLeader.clear()
      eventsToSendToLeader.foreach { e =>
        addEventsFromLocal(e)
      }

    } else {
      if (!runningStages.isEmpty) {
        /*
        eventsFromLeader.foreach {case(ep, es) =>
          allReceivedEventsIds ++= es.map {e => e.task.partitionId}
        }
        */
        val newReceived = getEventsFromFollower(false)
        val history = getEventsToHistory()
        pids.foreach { pid =>
          if (newReceived.isDefinedAt(pid)) {
            allReceivedEventsIds ++= newReceived(pid).map {e => e.task.partitionId}
          }
        }
        history.foreach { case(ep, es) =>
          allReceivedEventsIds ++= es.map {e => e.task.partitionId}
        }
      }
    }

    val partitionEachStage = new HashMap[Int, Seq[Int]]

    val checkStages = runningStages.clone()
    if (newLeader) {
      checkStages ++= waitingStagesWontClear
    }

    checkStages.foreach{ stage =>
      var subPartitions: Seq[Int] = Seq.empty
      pids.foreach { pid =>
        if (sInfo.partners(pid).subPartitions.isDefinedAt(stage.id)) {
          subPartitions ++= sInfo.partners(pid).subPartitions(stage.id)
        }
      }
      logInfo(s"in stage ${stage.id}, " +
        s"failed partners ${pids} handle subPartitions: ${subPartitions.sorted}")
      // logInfo(s"allReceivedEventsIds: ${allReceivedEventsIds.toSeq.sorted}")
      logInfo(s"stage ${stage.id} pendingPartitions: ${stage.pendingPartitions.toSeq.sorted}")
      // val partitionAdditional = subPartitions.diff(allReceivedEventsIds.toSeq)
      val partitionAdditional = stage.pendingPartitions.intersect(subPartitions.toSet).toSeq
      if (!partitionAdditional.isEmpty) {
        if (runningStages.contains(stage)) {
          submitMissingTasksAdditional(stage, jobId, partitionAdditional)
        } else {
          logInfo(s"partitionAdditional to submit later: ${partitionAdditional.sorted}")
        }
        partitionEachStage.put(stage.id, partitionAdditional)
      } else {
        logInfo(s"stage ${stage.id} has no additional partition to submit")
      }
    }

    syncInfoLock.synchronized {
      partitionEachStage.foreach {case(stageId, sp) =>
          syncInfo.addPartition(stageId, id, sp)
      }
      pids.foreach {pid =>
      syncInfo.partners(pid).clearSubPartitions()
      resubmitPartnerApplication(pid, syncInfo.partners(pid).clusterName)
      }
      zkClient.updateSyncInfo(syncInfo)
    }
  }

  def resubmitPartnerApplication(pid: Int, clusterName: String): Unit = {
    logInfo(s"resubmitPartner $pid to cluster $clusterName")
    val zkSparkRecoveryCentre =
      new ZkSparkRecoveryCentre(sc.getConf.get("spark.zk.hosts"), groupName)
    zkSparkRecoveryCentre.putRecoveryTask(clusterName, System.currentTimeMillis() + "")
  }

  // (leaderId, leaderEndPoint)
  def registerAsFollower(lId: Int, endPoint: RpcEndpointRef, leaderUrl: String): Boolean = {
    // val (leaderId, endpoint) = syncInfo.getLeaderInfo()
    if (lId == id) {
      // TODO:
      isLeader = true
      false
    } else if (lId == -1) {
      // TODO: call zk to election
      false
    } else {
      taskScheduler.registerAsFollower(id, endPoint, leaderUrl)
    }
  }

  def handleRecoverApplicationOnLeader(followerId: Int): RecoverInfo = {
    logInfo(s"receve recover request from: ${followerId}")
    syncInfoLock.synchronized {
      syncInfo.setFollowerState(followerId, ApplicationMasterState.RECOVERING)
      zkClient.updateSyncInfo(syncInfo)
    }
    recoverInfo
  }

  def getRecoverInfoFromLeader(): RecoverInfo = {
    taskScheduler.getRecoverInfoFromLeader(id)
  }

  def sendRemoteEventsToLeader(): Unit = {
    var ask = new HashSet[(Int, Int)]
    askListLock.synchronized {
      ask = askList.clone()
      askList.clear()
    }
    val events = getEventsToSendToLeader()
    if (!events.isEmpty || !ask.isEmpty) {
      addEventsToHistory(ceEpoch, events)
      logInfo(s"sendRemoteEventsToLeader: ${leaderId}, epoch: ${ceEpoch}")
      if(!ask.isEmpty) {
        logInfo(s"ask for events: ${ask.toString()}")
      }
      try {
        taskScheduler.sendRemoteEventsToLeader(id, ceEpoch, events, ask)
      } catch {
        case e: ConnectException =>
          logInfo("ConnectException")
        case e: IOException =>
          logInfo(s"IOException: $e")
      }

    }
  }

  // called every few seconds
  def sendRemoteEventsToFollowers(): Unit = {
    val events = getEventsToSendOnLeader(true)
    // val allEventsIds = new HashSet[Int]
    val allEventsIds = new mutable.HashSet[(Int, Int)]
    getEventsToHistory().foreach { case(ep, es) =>
      allEventsIds ++= es.map(e => (e.task.stageId, e.task.partitionId))
    }

    var hasEvents = false
    events.foreach {case(fid, es) =>
        if (!es.isEmpty) {
          hasEvents = true
        }
    }

    if (hasEvents || true) {
      ceEpoch += 1
      zkClient.updateEpoch(ceEpoch)
      logInfo(s"sendRemoteEventsToFollowers, epoch: ${ceEpoch}")
      try {
        taskScheduler.sendRemoteEventsToFollowers(ceEpoch, events, allEventsIds)
      } catch {
        case e: ConnectException =>
          logInfo("ConnectException")
        case e: IOException =>
          logInfo("IOException")
      }

    }
  }

  def handleRemoteEventsFromLeader(epoch: Long,
                                   events: HashSet[CompletionEvent],
                                   allEventsIds: HashSet[(Int, Int)]): Unit = {
    // AccumulatorContext.setOriginals(getSyncInfo().originals)
    if (eventsFromLeader.isDefinedAt(epoch)) {
      logInfo(s"remote events from leader with epoch ${epoch} already got")
      return
    }
    logInfo(s"remote events from leader with epoch ${epoch}")
    ceEpoch = epoch
    zkClient.updateEpoch(ceEpoch)
    eventsFromLeader.put(epoch, events)
    addEventsToHistory(ceEpoch, events)

    events.foreach{ e =>
      if (stagesReadyForEvents.contains(e.task.stageId)) {
        handleTaskCompletion(e, true)
      } else {
        eventsToHandleLock.synchronized {
          eventsToHandle += e
        }
      }
    }

    val localEvents = getEventsFromLocal(false).map{ e =>
      ((e.task.stageId, e.task.partitionId), e)}.toMap

    val eventsLeaderHasNot = localEvents.keySet.diff(allEventsIds)
    eventsLeaderHasNot.foreach { id =>
      addEventToSendOnFollower(localEvents(id))
    }
    logInfo(s"allEventsIds from leader: ${allEventsIds.toSeq.sorted.toString()}")
    logInfo(s"have events leader has not: ${eventsLeaderHasNot.toSeq.sorted.toString()}")

    var allReceivedEventsIds = localEvents.keySet
    eventsFromLeader.foreach {case(ep, es) =>
      allReceivedEventsIds ++= es.map {e => (e.task.stageId, e.task.partitionId)}
    }
    val eventsIHasNot = allEventsIds.diff(allReceivedEventsIds)
    askListLock.synchronized {
      askList ++= eventsIHasNot
    }
    logInfo(s"have events I have not: ${eventsIHasNot.toSeq.sorted.toString()}")
  }

  def handleRemoteEventsFromFollower(followerId: Int,
                                     epoch: Long,
                                     events: HashSet[CompletionEvent],
                                     ask: HashSet[(Int, Int)]): Unit = {
    if (epoch != ceEpoch) {
      // TODO: send history events to it
    }
    askListLock.synchronized {
      askFromFollower(followerId) = ask
    }
    logInfo(s"remote events from ${followerId}, with ask ${ask.toString()}, epoch: ${epoch}")
    handleTaskCompletionFromRemote(followerId, events)
  }

  def handleNewStageEventFromFollower(followerId: Int, jobId: Int, stageId: Int): Unit = {
    stageLock.synchronized {
      partnersStage(followerId) = (jobId, stageId)
    }
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, filesLost)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletionFromLocal(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
