package com.balihoo.fulfillment.workers

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.UUID.randomUUID

import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition}

import scala.sys.process._
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import play.api.libs.json._

import com.balihoo.fulfillment.adapters._

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json.{Json, JsObject}
import com.balihoo.fulfillment.util.Getch

abstract class FulfillmentWorker(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter) {

  val instanceId = randomUUID().toString

  val domain = swfAdapter.domain
  val name = validateSWFIdentifier(swfAdapter.config.getString("name"), 256)
  val version = validateSWFIdentifier(swfAdapter.config.getString("version"), 64)
  val taskListName = validateSWFIdentifier(name+version, 256)

  val defaultTaskHeartbeatTimeout = swfAdapter.config.getString("default_task_heartbeat_timeout")
  val defaultTaskScheduleToCloseTimeout = swfAdapter.config.getString("default_task_schedule_to_close_timeout")
  val defaultTaskScheduleToStartTimeout = swfAdapter.config.getString("default_task_schedule_to_start_timeout")
  val defaultTaskStartToCloseTimeout = swfAdapter.config.getString("default_task_start_to_close_timeout")

//  TODO This can be used to have the worker discover the address of the ec2 instance it is running on (if it is!)
//  This would be a nice piece of information to put into dynamo so we can log in and administer via ssh
//  without having to look up the instance in the ec2 dashboard.
  val hostAddress = sys.env.get("EC2_HOME") match {
    case Some(s:String) =>
      val aws_ec2_identify = "curl -s http://169.254.169.254/latest/meta-data/public-hostname"
      aws_ec2_identify.!!
    case None =>
      java.net.InetAddress.getLocalHost.getHostName
  }

  val workerTable = new FulfillmentWorkerTable(dynamoAdapter)

  val entry = new FulfillmentWorkerEntry
  entry.setInstance(instanceId)
  entry.setHostAddress(hostAddress)
  entry.setActivityName(name)
  entry.setActivityVersion(version)
  entry.setDomain(domain)
  entry.setStatus("--")
  entry.setStart(UTCFormatter.format(new Date()))

  val taskList: TaskList = new TaskList().withName(taskListName)

  val taskReq: PollForActivityTaskRequest = new PollForActivityTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  var completedTasks:Int = 0
  var failedTasks:Int = 0
  var canceledTasks:Int = 0

  var updateCounter:Int = 0

  declareWorker()

  var task:ActivityTask = null


  def work() = {

    registerActivityType()

    updateStatus("Starting")

    var done = false
    val getch = new Getch
    getch.addMapping(Seq("q", "Q", "Exit"), () => {println("quit");done = true})

    getch.doWith {
      while(!done) {
        print(".")

        updateStatus("Polling")
        task = new ActivityTask
        try {
          task = swfAdapter.client.pollForActivityTask(taskReq)
          if(task.getTaskToken != null) {
            updateStatus("Processing task..")
            try {
              handleTask(new ActivityParameters(task.getInput))
            } catch {
              case e: Exception =>
                failTask("Exception", e.getMessage)
            }
          }
        } catch {
          case e:Exception =>
            println("\n"+e.getMessage)
          case t:Throwable =>
            println("\n"+t.getMessage)
        }
      }
    }
    updateStatus("Exiting")
    print("Exiting...")
  }

  def handleTask(params:ActivityParameters)

  def withTaskHandling(code: => String) {
    try {
      val result:String = code
      completeTask(s"""{"$name": "$result"}""")
    } catch {
      case exception:Exception =>
        failTask(s"""{"$name": "${exception.toString}"}""", exception.getMessage)
      case t:Throwable =>
        failTask(s"""{"$name": "Caught a Throwable"}""", t.getMessage)
    }
  }

  def declareWorker() = {
    entry.setLast(UTCFormatter.format(new Date()))
    workerTable.insert(entry)
  }

  def updateStatus(status:String) = {
    updateCounter += 1
    entry.setLast(UTCFormatter.format(new Date()))
    entry.setStatus(status)
    workerTable.update(entry)
  }

  def completeTask(result:String) = {
    completedTasks += 1
    val response:RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest
    response.setTaskToken(task.getTaskToken)
    response.setResult(result)
    swfAdapter.client.respondActivityTaskCompleted(response)
    updateStatus(s"Completed Task")
  }

  def failTask(reason:String, details:String) = {
    failedTasks += 1
    val response:RespondActivityTaskFailedRequest = new RespondActivityTaskFailedRequest
    response.setTaskToken(task.getTaskToken)
    response.setReason(reason)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskFailed(response)
    updateStatus(s"Failed Task: $reason $details")
  }

  def cancelTask(details:String) = {
    canceledTasks += 1
    val response:RespondActivityTaskCanceledRequest = new RespondActivityTaskCanceledRequest
    response.setTaskToken(task.getTaskToken)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskCanceled(response)
    updateStatus(s"Cancel Task: $details")
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

  def hasParameter(param:String) = {
    params contains param
  }

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

object UTCFormatter {

  val SEC_IN_MS = 1000
  val MIN_IN_MS = SEC_IN_MS * 60
  val HOUR_IN_MS = MIN_IN_MS * 60
  val DAY_IN_MS = HOUR_IN_MS * 24

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  def format(date:Date): String = {
    dateFormat.format(date)
  }

}

class FulfillmentWorkerTable(val dynamoAdapter:DynamoAdapter) {

  def insert(entry:FulfillmentWorkerEntry) = {
    dynamoAdapter.put(entry.getDynamoItem)
  }

  def update(entry:FulfillmentWorkerEntry) = {
    dynamoAdapter.update(entry.getDynamoUpdate)
  }

  def get() = {
    val scanExp:DynamoDBScanExpression = new DynamoDBScanExpression()

    val oldest = UTCFormatter.format(new Date(System.currentTimeMillis() - UTCFormatter.DAY_IN_MS))

    scanExp.addFilterCondition("last",
      new Condition()
        .withComparisonOperator(ComparisonOperator.GT)
        .withAttributeValueList(new AttributeValue().withS(oldest)))

    val now = new Date()

    val list = dynamoAdapter.mapper.scan(classOf[FulfillmentWorkerEntry], scanExp)
    for(worker:FulfillmentWorkerEntry <- list) {
      worker.minutesSinceLast = (now.getTime - UTCFormatter.dateFormat.parse(worker.last).getTime) / UTCFormatter.MIN_IN_MS
    }

    list.toList
  }

}

@DynamoDBTable(tableName="fulfillment_worker_status")
class FulfillmentWorkerEntry {
  var instance:String = ""
  var hostAddress:String = ""
  var domain:String = ""
  var activityName:String = ""
  var activityVersion:String = ""
  var status:String = ""
  var start:String = ""
  var last:String = ""

  var minutesSinceLast:Long = 0

  def toJson:JsValue = {
    Json.toJson(Map(
      "instance" -> Json.toJson(instance),
      "hostAddress" -> Json.toJson(hostAddress),
      "domain" -> Json.toJson(domain),
      "activityName" -> Json.toJson(activityName),
      "activityVersion" -> Json.toJson(activityVersion),
      "status" -> Json.toJson(status),
      "start" -> Json.toJson(start),
      "last" -> Json.toJson(last),
      "minutesSinceLast" -> Json.toJson(minutesSinceLast)
    ))

  }

  @DynamoDBHashKey(attributeName="instance")
  def getInstance():String = { instance }
  def setInstance(ins:String) = { this.instance = ins }

  @DynamoDBHashKey(attributeName="hostAddress")
  def getHostAddress:String = { hostAddress }
  def setHostAddress(ha:String) = { this.hostAddress = ha }

  @DynamoDBAttribute(attributeName="domain")
  def getDomain:String = { domain }
  def setDomain(domain:String) { this.domain = domain; }

  @DynamoDBAttribute(attributeName="activityName")
  def getActivityName:String = { activityName }
  def setActivityName(activityName:String) { this.activityName = activityName; }

  @DynamoDBAttribute(attributeName="activityVersion")
  def getActivityVersion:String = { activityVersion }
  def setActivityVersion(activityVersion:String) { this.activityVersion = activityVersion; }

  @DynamoDBAttribute(attributeName="status")
  def getStatus:String = { status }
  def setStatus(status:String) { this.status = status; }

  @DynamoDBAttribute(attributeName="start")
  def getStart:String = { start }
  def setStart(start:String) { this.start = start; }

  @DynamoDBAttribute(attributeName="last")
  def getLast:String = { last }
  def setLast(last:String) { this.last = last; }

  def getDynamoItem:DynamoItem = {
    new DynamoItem("fulfillment_worker_status")
      .addString("instance", instance)
      .addString("hostAddress", hostAddress)
      .addString("domain", domain)
      .addString("activityName", activityName)
      .addString("activityVersion", activityVersion)
      .addString("status", status)
      .addString("start", start)
      .addString("last", last)
  }

  def getDynamoUpdate:DynamoUpdate = {
    new DynamoUpdate("fulfillment_worker_status")
      .forKey("instance", instance)
      .addString("status", status)
      .addString("last", last)
  }

}

