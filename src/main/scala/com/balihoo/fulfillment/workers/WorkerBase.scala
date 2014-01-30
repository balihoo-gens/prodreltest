package com.balihoo.fulfillment.workers

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.simpleworkflow.model._
import java.net.SocketException
import com.balihoo.fulfillment.config.WorkflowConfig

/**
 * Required configs for each worker
 */
abstract class WorkerBaseConfig {
  /*** Abstracts ***/
  val activityType: ActivityType
  val taskList: TaskList

  val taskScheduleToClose: Int
  val taskScheduleToStart: Int
  val taskStartToClose: Int
  val taskHeartbeatTimeout: Int
  val taskDescription: String

  /*** Common ***/
  def registerActivityType() {
    println("checking that activity type " + activityType.getName + ", version " + activityType.getVersion + " exists")

    try {
      val describeActivityType: DescribeActivityTypeRequest = new DescribeActivityTypeRequest
      describeActivityType.setActivityType(activityType)
      describeActivityType.setDomain(WorkflowConfig.domain)
      WorkflowConfig.client.describeActivityType(describeActivityType)
      println("activity type already exists")
    } catch {
      case e: com.amazonaws.services.simpleworkflow.model.UnknownResourceException => {
        println("activity type is new, registering")
        val registerActivityType: RegisterActivityTypeRequest = new RegisterActivityTypeRequest()
        registerActivityType.setDomain(WorkflowConfig.domain)
        registerActivityType.setName(activityType.getName)
        registerActivityType.setVersion(activityType.getVersion)
        registerActivityType.setDescription(taskDescription)
        registerActivityType.setDefaultTaskList(taskList)
        registerActivityType.setDefaultTaskScheduleToCloseTimeout(taskScheduleToClose.toString)
        registerActivityType.setDefaultTaskScheduleToStartTimeout(taskScheduleToStart.toString)
        registerActivityType.setDefaultTaskStartToCloseTimeout(taskStartToClose.toString)
        registerActivityType.setDefaultTaskHeartbeatTimeout(taskHeartbeatTimeout.toString)

        //todo: might should deprecate the old one
        WorkflowConfig.client.registerActivityType(registerActivityType)
      }
    }
  }
}

abstract class WorkerBase {

  /*** Abstracts ***/
  /*
  * perform any work for the given task
  * When complete, something needs to call respondTaskComplete with the initiating task token and result.
  * The specific implementation of worker can respond whenever it likes. It can call the responder immediately,
  * spawn a thread to monitor an open connection, or add a remote result token to a list of things to poll.
  */
  def doWork(task: ActivityTask)
  //handle a signal that the workflow task has been cancelled
  def cancelTask(taskToken: String)
  def getConfig: WorkerBaseConfig

  /*** Common ***/
  val workflowConfig = WorkflowConfig
  val client = workflowConfig.client

  val heartbeatResponseHandler: HeartbeatResponseHandler = new HeartbeatResponseHandler(this)

  //todo: move everything but this to an actor
  def pollForWorkerTasks() = {
    val taskReq = new PollForActivityTaskRequest()
    taskReq.setDomain(workflowConfig.domain)
    taskReq.setTaskList(getConfig.taskList)

    while(true) {
      try{
        val task: ActivityTask = client.pollForActivityTask(taskReq)
        //        println("got back an activity task: " + task)

        if (task.getActivityId != null) {
          doWork(task)
        }
      } catch {
        case e: SocketException => {
          if (e.getMessage.equals("Connection reset")) {
            println("connection reset was caught and ignored")
          } else {
            throw e
          }
        }
        case e: Throwable => println("Unexpected exception " + e)
      }
    }
  }

  /**
   * Issue a heartbeat response.  Although the heartbeat method is standard, workers might have different
   * timings on issuing them, or might not need to at all.
   * @param taskToken
   * @param details
   * @return
   */
  def issueHeartbeatAsync(taskToken: String, details: String = null) = {
    val heartbeat: RecordActivityTaskHeartbeatRequest = new RecordActivityTaskHeartbeatRequest
    heartbeat.setTaskToken(taskToken)
    if (details != null) {
      heartbeat.setDetails(details)
    }

    //    println("HB - replying with heartbeat")
    client.recordActivityTaskHeartbeatAsync(heartbeat, heartbeatResponseHandler)
  }

  /**
   * Response when the task completes.
   * @param taskToken The task token for the completed task.
   * @param result The result of the task
   */
  def respondTaskComplete(taskToken: String, result:String) = {
    val taskCompletedRequest: RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest()
    taskCompletedRequest.setTaskToken(taskToken)
    taskCompletedRequest.setResult(result)

    //    println("responding with task complete")
    workflowConfig.client.respondActivityTaskCompleted(taskCompletedRequest)
  }
}

/**
 * Receive async responses from heartbeats.  These include whether the task has been cancelled since first being
 * issued.  We will respond to that news by calling the worker's cancelTask method.
 * @param worker The worker object on which to call cancelTask.
 */
class HeartbeatResponseHandler(worker: WorkerBase) extends AsyncHandler[RecordActivityTaskHeartbeatRequest, ActivityTaskStatus] {
  def onError(e: Exception): Unit = {}

  def onSuccess(request: RecordActivityTaskHeartbeatRequest, taskStatus: ActivityTaskStatus): Unit = {
    if (taskStatus.getCancelRequested) {
      worker.cancelTask(request.getTaskToken)
    }
  }
}

