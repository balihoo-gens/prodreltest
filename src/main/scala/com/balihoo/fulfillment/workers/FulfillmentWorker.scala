package com.balihoo.fulfillment.workers

//scala imports
import scala.collection.mutable
import scala.sys.process._
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}
import scala.concurrent.{Future, future}
import scala.concurrent.ExecutionContext.Implicits.global

//java imports
import java.util.Date
import java.util.UUID.randomUUID
import java.util.concurrent.{Future => JFuture}
import java.util.concurrent.TimeUnit

//aws imports
import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ComparisonOperator, Condition}
import com.amazonaws.services.simpleworkflow.model._

//play imports
import play.api.libs.json._

//other external
import org.joda.time.{Minutes, DateTime}
import org.joda.time.format.ISODateTimeFormat
import org.keyczar.Crypter

//local imports
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._

abstract class FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  val instanceId = randomUUID().toString

  val domain = swfAdapter.domain
  val name = swfAdapter.name
  val version = swfAdapter.version
  val taskListName = swfAdapter.taskListName
  val taskList = swfAdapter.taskList

  val defaultTaskHeartbeatTimeout = swfAdapter.config.getString("default_task_heartbeat_timeout")
  val defaultTaskScheduleToCloseTimeout = swfAdapter.config.getString("default_task_schedule_to_close_timeout")
  val defaultTaskScheduleToStartTimeout = swfAdapter.config.getString("default_task_schedule_to_start_timeout")
  val defaultTaskStartToCloseTimeout = swfAdapter.config.getString("default_task_start_to_close_timeout")

  val hostAddress = sys.env.get("EC2_HOME") match {
    case Some(s:String) =>
      val url = "http://169.254.169.254/latest/meta-data/public-hostname"
      val aws_ec2_identify = "curl -s $url --max-time 2 --retry 3"
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
  entry.setStart(UTCFormatter.format(DateTime.now))

  var completedTasks:Int = 0
  var failedTasks:Int = 0
  var canceledTasks:Int = 0

  var updateCounter:Int = 0

  declareWorker()

  var _lastTaskToken: String = ""

  def work() = {

    registerActivityType()

    updateStatus("Starting")

    val specification = getSpecification

    var done = false
    val getch = new Getch
    getch.addMapping(
      Seq("q", "Q", "Exit"), () => {
        //stdin received quit request. Respond on stdout
        println("Quitting...")
        updateStatus("Terminated by user", "WARN")
        done = true
      }
    )
    //echo a dot
    getch.addMapping(Seq("."), () => {
      print(".")
    }

    getch.doWith {
      while(!done) {
        val jfut = swfAdapter.client
        val jfut = swfAdapter.client.pollForActivityTaskAsync(taskReq)
        if (pollopt.isEmpty || pollopt.get.isCompleted) {

          //create a future for the poll, so the loop stays responsive
          val pollfut = future {
            updateStatus("Polling")
            //SWF creates a Java future for
            while (!jfut.isDone) {
              if (done) {
                jfut.cancel(true)
                throw new Exception("User Termination")
              }
              if (jfut.isCancelled) {
                throw new Exception("Polling Cancelled")
              }
              //wait for the java future to complete or be interrupted
              Thread.sleep(100)
            }
            //polling is done here, return the task
            jfut.get
          }

          //store the new future
          pollopt = Some(pollfut)

          //complete handler for the polling future
          pollfut onComplete {
            case Success(task) =>
              _lastTaskToken = task.getTaskToken
              if(_lastTaskToken != null) {
                val shortToken = (_lastTaskToken takeRight 10)
                try {
                  updateStatus("Processing task.." + shortToken )
                  handleTask(specification.getParameters(task.getInput))
                } catch {
                  case e:Exception =>
                    failTask(s"""{"$name": "${e.toString}"}""", e.getMessage)
                }
              } else {
                splog.info("no task available")
              }
            case Failure(e) =>
              updateStatus(s"Polling Exception ${e.getMessage}", "EXCEPTION")
          }
        } else {
          //wait for our scala polling future to complete
          Thread.sleep(100)
        }
      }
    }
    updateStatus("Exiting")
    val excsvc = swfAdapter.client.getExecutorService()
    excsvc.shutdown
    if (!excsvc.awaitTermination(3, TimeUnit.SECONDS)) {
      val tasks = excsvc.shutdownNow()
      println("remaining tasks: " + tasks.length)
      swfAdapter.client.shutdown
    }
  }

  def getSpecification: ActivitySpecification

  def handleTask(params:ActivityParameters)

  def withTaskHandling(code: => String) {
    try {
      val result:String = code
      completeTask(s"""{"$name": "$result"}""")
    } catch {
      case e:Exception =>
        failTask(s"""{"$name": "${e.toString}"}""", e.getMessage)
      case t:Throwable =>
        failTask(s"""{"$name": "Caught a Throwable"}""", t.getMessage)
    }
  }

  def declareWorker() = {
    val status = s"Declaring $name $domain $taskListName"
    splog("INFO",status)
    entry.setLast(UTCFormatter.format(DateTime.now))
    entry.setStatus(status)
    workerTable.insert(entry)
  }

  def updateStatus(status:String, level:String="INFO") = {
    try {
      updateCounter += 1
      entry.setLast(UTCFormatter.format(DateTime.now))
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
    response.setTaskToken(_lastTaskToken)
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
    response.setTaskToken(_lastTaskToken)
    response.setReason(reason)
    response.setDetails(details)
    swfAdapter.client.respondActivityTaskFailed(response)
    _resolveTask(new TaskResolution("Failed", s"$reason $details"))
  }

  def cancelTask(details:String) = {
    canceledTasks += 1
    val response:RespondActivityTaskCanceledRequest = new RespondActivityTaskCanceledRequest
    response.setTaskToken(_lastTaskToken)
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
  val when = UTCFormatter.format(DateTime.now)
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

  def get(param: String): Option[String] = params.get(param)

  override def toString:String = {
    params.toString()
  }
}

object UTCFormatter {

  val SEC_IN_MS = 1000
  val MIN_IN_MS = SEC_IN_MS * 60
  val HOUR_IN_MS = MIN_IN_MS * 60
  val DAY_IN_MS = HOUR_IN_MS * 24

  val dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC()

  def format(dateTime:DateTime): String = {
    dateTimeFormatter.print(dateTime)
  }
  def format(date:Date): String = {
    dateTimeFormatter.print(new DateTime(date))
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

    val oldest = DateTime.now.minusDays(1)

    scanExp.addFilterCondition("last",
      new Condition()
        .withComparisonOperator(ComparisonOperator.GT)
        .withAttributeValueList(new AttributeValue().withS(UTCFormatter.format(oldest))))

    val list = dynamoAdapter.mapper.scan(classOf[FulfillmentWorkerEntry], scanExp)
    for(worker:FulfillmentWorkerEntry <- list) {
      worker.minutesSinceLast = Minutes.minutesBetween(DateTime.now, new DateTime(worker.last)).getMinutes
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

abstract class FulfillmentWorkerApp {
  def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker

  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog("INFO", s"Started $name")
    try {
      val cfg = PropertiesLoader(args, name)
      val worker = createWorker(cfg, splog)
      worker.work()
      println("done")
    }
    catch {
      case t:Throwable =>
        splog("ERROR", t.getMessage)
    }
    splog("INFO", s"Terminated $name")
    println("really done")
  }
}

