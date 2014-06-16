package com.balihoo.fulfillment.workers

import java.util.UUID.randomUUID

import scala.language.implicitConversions

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter}

import com.amazonaws.services.simpleworkflow.model._
import com.amazonaws.services.sqs.model.{GetQueueUrlRequest, SendMessageRequest}
import play.api.libs.json._

abstract class FulfillmentWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter) {

  val instanceId = randomUUID().toString

  val domain = swfAdapter.config.getString("domain")
  val name = swfAdapter.config.getString("name")
  val version = swfAdapter.config.getString("version")
  val taskListName = name+version
  val workerStatusQueue: String = sqsAdapter.config.getString("workerstatusqueue")

  val defaultTaskHeartbeatTimeout = swfAdapter.config.getString("default_task_heartbeat_timeout")
  val defaultTaskScheduleToCloseTimeout = swfAdapter.config.getString("default_task_schedule_to_close_timeout")
  val defaultTaskScheduleToStartTimeout = swfAdapter.config.getString("default_task_schedule_to_start_timeout")
  val defaultTaskStartToCloseTimeout = swfAdapter.config.getString("default_task_start_to_close_timeout")

  val taskList: TaskList = new TaskList().withName(taskListName)

  val taskReq: PollForActivityTaskRequest = new PollForActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  val queueUrlRequest = new GetQueueUrlRequest(workerStatusQueue)
  val statusQueue = sqsAdapter.client.getQueueUrl(queueUrlRequest)

  var completedTasks:Int = 0
  var failedTasks:Int = 0
  var canceledTasks:Int = 0

  def work() = {

    registerActivityType()

    while(true) {
      print(".")

      updateStatus("Polling")
      val task : ActivityTask = swfAdapter.client.pollForActivityTask(taskReq)
      if(task.getTaskToken != null) {
        updateStatus("Processing task..")
        try {
          handleTask(task)
        } catch {
          case e:Exception =>
            failTask(task.getTaskToken, "Exception", e.getMessage)
        }
      }
    }
  }

  def handleTask(task: ActivityTask)

  def baseMessage() = {
    var message: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]()
    message += ("instance" -> instanceId)
    message += ("type" -> "ActivityWorker")
    message += ("activityName" -> name)
    message += ("activityVersion" -> version)
    message += ("canceledTasks" -> canceledTasks.toString)
    message += ("failedTasks" -> failedTasks.toString)
    message += ("completedTasks" -> completedTasks.toString)
  }

  def updateStatus(status:String) = {
    var message = baseMessage()
    message += ("status" -> status)

    sendMessage(Json.stringify(Json.toJson(message.toMap)))
  }

  def sendMessage(message:String) = {
    val m = new SendMessageRequest(statusQueue.getQueueUrl, message)
    sqsAdapter.client.sendMessage(m)
  }

  def completeTask(token:String, result:String) = {
    completedTasks += 1
    val response:RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest
    response.setTaskToken(token)
    response.setResult(result)
    swfAdapter.client.respondActivityTaskCompleted(response)
  }

  def failTask(token:String, reason:String, details:String) = {
    failedTasks += 1
    val response:RespondActivityTaskFailedRequest = new RespondActivityTaskFailedRequest
    response.setTaskToken(token)
    response.setReason(reason)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskFailed(response)
  }

  def cancelTask(token:String, details:String) = {
    canceledTasks += 1
    val response:RespondActivityTaskCanceledRequest = new RespondActivityTaskCanceledRequest
    response.setTaskToken(token)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskCanceled(response)
  }

  def registerActivityType() = {
    val activityType = new ActivityType()
      .withName(name)
      .withVersion(version)

    val describe = new DescribeActivityTypeRequest()
      .withActivityType(activityType)
      .withDomain(domain)

    try {
      val detail = swfAdapter.client.describeActivityType(describe)
    } catch {
      case ure: UnknownResourceException =>
        updateStatus(s"Registering new Activity ($name,$version)")
        val registrationRequest = new RegisterActivityTypeRequest()
          .withDomain(domain)
          .withName(name)
          .withVersion(version)
          .withDefaultTaskList(taskList)
          .withDefaultTaskHeartbeatTimeout(defaultTaskHeartbeatTimeout)
          .withDefaultTaskScheduleToCloseTimeout(defaultTaskScheduleToCloseTimeout)
          .withDefaultTaskScheduleToStartTimeout(defaultTaskScheduleToStartTimeout)
          .withDefaultTaskStartToCloseTimeout(defaultTaskStartToCloseTimeout)
        swfAdapter.client.registerActivityType(registrationRequest)
      case e: Exception =>
        updateStatus(s"Failed to register Activity ($name,$version)")
    }

  }

  def getRequiredParameter(param:String, input:JsObject, inputRaw:String) = {
    if(!(input.keys contains param)) {
      throw new Exception(s"input parameter '$param' is REQUIRED! '$inputRaw' doesn't contain '$param'")
    }

    input.value(param).as[String]
  }

  def getOptionalParameter(param:String, input:JsObject, inputRaw:String, default:Any):Any = {
    if(!(input.keys contains param)) {
      return default
    }

    input.value(param).as[String]
  }

}

