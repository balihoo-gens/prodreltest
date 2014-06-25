package com.balihoo.fulfillment.workers

import java.util.UUID.randomUUID

import scala.language.implicitConversions

import com.balihoo.fulfillment.{SQSAdapter, SWFAdapter}

import com.amazonaws.services.simpleworkflow.model._
import com.amazonaws.services.sqs.model.{GetQueueUrlRequest, SendMessageRequest}
import play.api.libs.json.{Json, JsObject}

abstract class FulfillmentWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter) {

  val instanceId = randomUUID().toString

  val domain = swfAdapter.config.getString("domain")
  val name = validateSWFIdentifier(swfAdapter.config.getString("name"), 256)
  val version = validateSWFIdentifier(swfAdapter.config.getString("version"), 64)
  val taskListName = validateSWFIdentifier(name+version, 256)
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

  var task:ActivityTask = null

  def work() = {

    registerActivityType()

    updateStatus("Starting")

    while(true) {
      print(".")

      updateStatus("Polling")
      task = new ActivityTask
      task = swfAdapter.client.pollForActivityTask(taskReq)
      if(task.getTaskToken != null) {
        updateStatus("Processing task..")
        try {
          handleTask(new ActivityParameters(task.getInput))
        } catch {
          case e:Exception =>
            failTask("Exception", e.getMessage)
        }
      }
    }
  }

  def handleTask(params:ActivityParameters)

  def withTaskHandling(code: => String) {
    try {
      val result:String = code
      completeTask(s"""{"$name": "$result"}""")
    } catch {
      case exception:Exception =>
        failTask(s"""{"$name": "${exception.toString}"}""", exception.getMessage)
      case _:Throwable =>
        failTask(s"""{"$name": "Caught a Throwable""", "caught a throwable")
    }
  }

  def baseMessage() = {
    var message = collection.mutable.Map[String, String]()
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

  def completeTask(result:String) = {
    completedTasks += 1
    val response:RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest
    response.setTaskToken(task.getTaskToken)
    response.setResult(result)
    swfAdapter.client.respondActivityTaskCompleted(response)
  }

  def failTask(reason:String, details:String) = {
    failedTasks += 1
    val response:RespondActivityTaskFailedRequest = new RespondActivityTaskFailedRequest
    response.setTaskToken(task.getTaskToken)
    response.setReason(reason)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskFailed(response)
  }

  def cancelTask(details:String) = {
    canceledTasks += 1
    val response:RespondActivityTaskCanceledRequest = new RespondActivityTaskCanceledRequest
    response.setTaskToken(task.getTaskToken)
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
      swfAdapter.client.describeActivityType(describe)
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

  def validateSWFIdentifier(ident:String, length:Int) = {
    for(s <- Array("##", ":", "/", "|", "arn")) {
      if(ident.contains(s)) throw new Exception(s"$ident must not contain '$s'")
    }
    if(ident.length > length) throw new Exception(s"$ident must not be longer than '$length'")

    ident
  }

}

class ActivityParameters(input:String) {
  val inputObject:JsObject = Json.parse(input).as[JsObject]
  val params:Map[String, String] = (for((key, value) <- inputObject.fields) yield key -> value.as[String]).toMap

  def getRequiredParameter(param:String):String = {
    if(!(params contains param)) {
      throw new Exception(s"input parameter '$param' is REQUIRED! '$input' doesn't contain '$param'")
    }
    params(param)
  }

  def getOptionalParameter(param:String, default:String):String = {
    params.getOrElse(param, default)
  }

  override def toString = {
    input
  }
}

