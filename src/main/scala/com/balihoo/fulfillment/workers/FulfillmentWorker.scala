package com.balihoo.fulfillment.workers

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.util.UUID.randomUUID

import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition}
import com.balihoo.fulfillment.config.{SWFVersion, SWFName}
import org.keyczar.Crypter

import scala.collection.mutable
import scala.sys.process._
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import play.api.libs.json._

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json.{Json, JsObject}
import com.balihoo.fulfillment.util._

trait LoggingWorkflowAdapter
  extends SWFAdapterComponent
    with DynamoAdapterComponent
    with SploggerComponent
{}

trait LoggingWorkflowAdapterImpl
  extends LoggingWorkflowAdapter {
  val _cfg: PropertiesLoader
  val _splog: Splogger

  def splog = _splog

  lazy private val _swf = new SWFAdapter(_cfg)
  def swfAdapter = _swf

  lazy private val _dyn = new DynamoAdapter(_cfg)
  def dynamoAdapter = _dyn
}

abstract class FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  val instanceId = randomUUID().toString

  val domain = swfAdapter.domain
  val name = new SWFName(swfAdapter.config.getString("name"))
  val version = new SWFVersion(swfAdapter.config.getString("version"))
  val taskListName = new SWFName(name+version)

  val defaultTaskHeartbeatTimeout = swfAdapter.config.getString("default_task_heartbeat_timeout")
  val defaultTaskScheduleToCloseTimeout = swfAdapter.config.getString("default_task_schedule_to_close_timeout")
  val defaultTaskScheduleToStartTimeout = swfAdapter.config.getString("default_task_schedule_to_start_timeout")
  val defaultTaskStartToCloseTimeout = swfAdapter.config.getString("default_task_start_to_close_timeout")

  val hostAddress = sys.env.get("EC2_HOME") match {
    case Some(s:String) =>
      val aws_ec2_identify = "curl -s http://169.254.169.254/latest/meta-data/public-hostname"
      aws_ec2_identify.!!
    case None =>
      java.net.InetAddress.getLocalHost.getHostName
  }

  val workerTable = new FulfillmentWorkerTable with DynamoAdapterComponent {
    def dynamoAdapter = FulfillmentWorker.this.dynamoAdapter
  }

  val taskResolutions = new mutable.Queue[TaskResolution]()

  val entry = new FulfillmentWorkerEntry
  entry.setInstance(instanceId)
  entry.setHostAddress(hostAddress)
  entry.setActivityName(name)
  entry.setActivityVersion(version)
  entry.setSpecification(getSpecification.toString)
  entry.setDomain(domain)
  entry.setStatus("--")
  entry.setResolutionHistory("[]")
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

    val specification = getSpecification

    var done = false
    val getch = new Getch
    getch.addMapping(
      Seq("q", "Q", "Exit"), () => {
        updateStatus("Terminated by user", "WARN")
        done = true
      }
    )

    getch.doWith {
      while(!done) {
        updateStatus("Polling")
        task = new ActivityTask
        try {
          task = swfAdapter.client.pollForActivityTask(taskReq)
          if(task.getTaskToken != null) {
            updateStatus("Processing task..")
            try {
              handleTask(specification.getParameters(task.getInput))
            } catch {
              case e: Exception =>
                failTask("Exception", e.getMessage)
            }
          }
        } catch {
          case e:Exception =>
            updateStatus("Polling Exception ${e.getMessage}", "EXCEPTION")
          case t:Throwable =>
            updateStatus("Polling Throwable ${t.getMessage}", "ERROR")
        }
      }
    }
    updateStatus("Exiting")
  }

  def getSpecification: ActivitySpecification

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
    val status = s"Declaring $name $domain $taskListName"
    splog("INFO",status)
    entry.setLast(UTCFormatter.format(new Date()))
    entry.setStatus(status)
    workerTable.insert(entry)
  }

  def updateStatus(status:String, level:String="INFO") = {
    try {
      updateCounter += 1
      entry.setLast(UTCFormatter.format(new Date()))
      entry.setStatus(status)
      workerTable.update(entry)
      splog(level,status)
    } catch {
      case e:Exception =>
        //splog will print to stdout on any throwable, or log to the default logfile
        splog("ERROR", s"$name failed to update status: ${e.toString}")
    }
  }

  private def _resolveTask(resolution:TaskResolution) = {
    if(taskResolutions.size == 20) {
      taskResolutions.dequeue()
    }
    taskResolutions.enqueue(resolution)
    entry.setResolutionHistory(Json.stringify(
      Json.toJson(for(res <- taskResolutions) yield res.toJson)
    ))
    updateStatus(resolution.resolution)
  }

  def completeTask(result:String) = {
    completedTasks += 1
    val response:RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest
    response.setTaskToken(task.getTaskToken)
    response.setResult(getSpecification.result.sensitive match {
      case true => getSpecification.crypter.encrypt(result)
      case _ => result
    })
    swfAdapter.client.respondActivityTaskCompleted(response)
    _resolveTask(new TaskResolution("Completed", result))
  }

  def failTask(reason:String, details:String) = {
    failedTasks += 1
    val response:RespondActivityTaskFailedRequest = new RespondActivityTaskFailedRequest
    response.setTaskToken(task.getTaskToken)
    response.setReason(reason)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskFailed(response)
    _resolveTask(new TaskResolution("Failed", s"$reason $details"))
  }

  def cancelTask(details:String) = {
    canceledTasks += 1
    val response:RespondActivityTaskCanceledRequest = new RespondActivityTaskCanceledRequest
    response.setTaskToken(task.getTaskToken)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskCanceled(response)
    _resolveTask(new TaskResolution("Canceled", details))
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

}

class TaskResolution(val resolution:String, val details:String) {
  val when = UTCFormatter.format(new Date())
  def toJson:JsValue = {
    Json.toJson(Map(
      "resolution" -> Json.toJson(resolution),
      "when" -> Json.toJson(when),
      "details" -> Json.toJson(details)
    ))
  }
}

class ActivityResult(val rtype:String, description:String, val sensitive:Boolean = false) {
  def toJson:JsValue = {
    Json.toJson(Map(
      "type" -> Json.toJson(rtype),
      "description" -> Json.toJson(description),
      "sensitive" -> Json.toJson(sensitive)
    ))
  }
}

class ActivityParameter(val name:String, val ptype:String, val description:String, val required:Boolean = true, val sensitive:Boolean = false) {
  var value:Option[String] = None

  def toJson:JsValue = {
    Json.toJson(Map(
      "name" -> Json.toJson(name),
      "type" -> Json.toJson(ptype),
      "description" -> Json.toJson(description),
      "required" -> Json.toJson(required),
      "sensitive" -> Json.toJson(sensitive)
    ))
  }
}

class ActivitySpecification(val params:List[ActivityParameter], val result:ActivityResult) {

  val crypter = new Crypter("config/crypto")
  val paramsMap:Map[String,ActivityParameter] = (for(param <- params) yield param.name -> param).toMap

  def getSpecification:JsValue = {
    Json.toJson(Map(
      "parameters" -> Json.toJson((for(param <- params) yield param.name -> param.toJson).toMap),
      "result" -> result.toJson
    ))
  }

  override def toString:String = {
    Json.stringify(getSpecification)
  }

  def getParameters(input:String):ActivityParameters = {
    val inputObject:JsObject = Json.parse(input).as[JsObject]
    for((name, value) <- inputObject.fields) {
      if(paramsMap contains name) {
        val param = paramsMap(name)
        param.value = Some(
          if(param.sensitive)
            crypter.decrypt(value.as[String])
          else
            value.as[String]
        )
      }
    }
    for(param <- params) {
      if(param.required && param.value.isEmpty) {
        throw new Exception(s"input parameter '${param.name}' is REQUIRED!")
      }
    }

    new ActivityParameters(
      (for((name, param) <- paramsMap if param.value.isDefined) yield param.name -> param.value.get).toMap
      ,input)
  }

}

class ActivityParameters(val params:Map[String,String], val input:String = "{}") {

  def has(param:String):Boolean = {
    params contains param
  }

  def apply(param:String):String = {
    params(param)
  }

  def getOrElse(param:String, default:String):String = {
    if(has(param)) {
      return params(param)
    }
    default
  }

  override def toString:String = {
    params.toString()
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

class FulfillmentWorkerTable {
  this: DynamoAdapterComponent =>

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
  var specification:String = ""
  var status:String = ""
  var resolutionHistory:String = ""
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
      "specification" -> Json.toJson(specification),
      "status" -> Json.toJson(status),
      "resolutionHistory" -> Json.toJson(resolutionHistory),
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

  @DynamoDBAttribute(attributeName="specification")
  def getSpecification:String = { specification }
  def setSpecification(specification:String) { this.specification = specification; }

  @DynamoDBAttribute(attributeName="status")
  def getStatus:String = { status }
  def setStatus(status:String) { this.status = status; }

  @DynamoDBAttribute(attributeName="resolutionHistory")
  def getResolutionHistory:String = { resolutionHistory }
  def setResolutionHistory(resolutionHistory:String) { this.resolutionHistory = resolutionHistory; }

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
      .addString("specification", specification)
      .addString("status", status)
      .addString("resolutionHistory", resolutionHistory)
      .addString("start", start)
      .addString("last", last)
  }

  def getDynamoUpdate:DynamoUpdate = {
    new DynamoUpdate("fulfillment_worker_status")
      .forKey("instance", instance)
      .addString("status", status)
      .addString("resolutionHistory", resolutionHistory)
      .addString("last", last)
  }

}

