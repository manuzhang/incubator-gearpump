/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.services.util

import org.apache.gearpump.cluster.AppMasterToMaster.{GeneralAppMasterSummary, MasterData}
import org.apache.gearpump.cluster.{AppJar, ApplicationStatus}
import org.apache.gearpump.cluster.ClientToMaster.CommandResult
import org.apache.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, HistoryMetricsItem, LastFailure, SubmitApplicationResult}
import org.apache.gearpump.cluster.master.MasterSummary
import org.apache.gearpump.cluster.worker.{ExecutorSlots, WorkerId, WorkerSummary}
import org.apache.gearpump.jarstore.FilePath
import org.apache.gearpump.metrics.Metrics.MetricType
import org.apache.gearpump.services.MasterService.{AppSubmissionResult, BuiltinPartitioners, SubmitApplicationRequest}
import org.apache.gearpump.services.{AppMasterService, MasterService, SecurityService, SupervisorService}
import org.apache.gearpump.streaming.AppMasterToMaster.StallingTasks
import org.apache.gearpump.streaming.ProcessorId
import org.apache.gearpump.streaming.appmaster.DagManager.{DAGOperationFailed, DAGOperationResult, DAGOperationSuccess}
import org.apache.gearpump.streaming.appmaster.StreamAppMasterSummary
import org.apache.gearpump.streaming.executor.Executor.ExecutorSummary
import org.apache.gearpump.streaming.task.TaskId
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.HistoryMetricsService.HistoryMetricsConfig
import upickle.default._
import upickle.Js

import scala.util.Try

object UpickleUtil {

  implicit val graphReader: Reader[Graph[Int, String]] = {
    reader[Js.Obj].map {
      case Js.Obj(graph) =>
        println(graph)
        // val vertexList = upickle.default.readJs[List[Int]]()
        // val edgeList = upickle.default.readJs[List[(Int, String, Int)]](edges)
        // Graph(vertexList, edgeList)
        Graph.empty[Int, String]
    }
  }

  implicit val workerIdReadWriter: ReadWriter[WorkerId] = readwriter[Js.Str].bimap[WorkerId](
    id => Js.Str(WorkerId.render(id)),
    json => WorkerId.parse(json.value)
  )

  implicit val appStatusReadWriter: ReadWriter[ApplicationStatus] =
    readwriter[Js.Str].bimap[ApplicationStatus](
      status => Js.Str(status.toString),
        json => json.value match {
          case "pending" => ApplicationStatus.PENDING
          case "active" => ApplicationStatus.ACTIVE
          case "succeeded" => ApplicationStatus.SUCCEEDED
          case "failed" => ApplicationStatus.FAILED
          case "terminated" => ApplicationStatus.TERMINATED
          case _ => ApplicationStatus.NONEXIST
        }
    )

  implicit val dagOperationResultReader: Reader[DAGOperationResult] =
    Reader.merge[DAGOperationResult](dagOperationSuccessReader, dagOperationFailedReader)

  implicit val dagOperationSuccessReader: Reader[DAGOperationSuccess.type] = macroR
  implicit val dagOperationFailedReader: Reader[DAGOperationFailed] = macroR

  implicit val historyMetricsWriter: Writer[HistoryMetrics] = macroW[HistoryMetrics]

  implicit val historyMetricsItemWriter: Writer[HistoryMetricsItem] = macroW[HistoryMetricsItem]

  implicit val metricsTypeWriter: Writer[MetricType] = macroW[MetricType]

  implicit val workerSummaryWriter: Writer[WorkerSummary] = macroW[WorkerSummary]

  implicit val historyMetricsConfigWriter: Writer[HistoryMetricsConfig] =
    macroW[HistoryMetricsConfig]

  implicit val executorSlotsWriter: Writer[ExecutorSlots] = macroW[ExecutorSlots]

  implicit val commandResultWriter: Writer[CommandResult] = macroW[CommandResult]

  implicit val statusWriter: Writer[SupervisorService.Status] = macroW[SupervisorService.Status]

  implicit val pathWriter: Writer[SupervisorService.Path] = macroW[SupervisorService.Path]

  implicit val userWriter: Writer[SecurityService.User] = macroW[SecurityService.User]

  implicit val builtinPartitionersWriter: Writer[BuiltinPartitioners] = macroW[BuiltinPartitioners]

  implicit val appJarWriter: Writer[AppJar] = macroW[AppJar]

  implicit val filePathWriter: Writer[FilePath] = macroW[FilePath]

  implicit val submitApplicationResultReader: Reader[SubmitApplicationResult] =
    macroR[SubmitApplicationResult]

  implicit val appSubmissionResultWriter: Writer[AppSubmissionResult] = macroW[AppSubmissionResult]

  implicit val appMastersDataWriter: Writer[AppMastersData] = macroW[AppMastersData]

  implicit val masterData: Writer[MasterData] = macroW[MasterData]

  implicit val masterSummaryWriter: Writer[MasterSummary] = macroW[MasterSummary]

  implicit val appMasterDataWriter: Writer[AppMasterData] = macroW[AppMasterData]

  implicit val streamAppMasterSummaryWriter: Writer[StreamAppMasterSummary] =
    macroW[StreamAppMasterSummary]

  implicit val graphWriter: Writer[Graph[ProcessorId, String]] =
    macroW[Graph[ProcessorId, String]]

  implicit val generalAppMasterSummaryWriter: Writer[GeneralAppMasterSummary] =
    macroW[GeneralAppMasterSummary]

  implicit val executorSummaryWriter: Writer[ExecutorSummary] =
    macroW[ExecutorSummary]

  implicit val taskIdWriter: Writer[TaskId] = macroW[TaskId]

  implicit val appMasterServiceStatusWriter: Writer[AppMasterService.Status] =
    macroW[AppMasterService.Status]

  implicit val lastFailureWriter: Writer[LastFailure] = macroW[LastFailure]

  implicit val stallingTasksWriter: Writer[StallingTasks] = macroW[StallingTasks]

  implicit val submitApplicationRequestReader: Reader[SubmitApplicationRequest] =
    macroR[SubmitApplicationRequest]

  implicit val masterServiceStatusWriter: Writer[MasterService.Status] =
    macroW[MasterService.Status]

}