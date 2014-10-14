package com.balihoo.fulfillment.workers

//scala imports
import scala.collection.mutable
import scala.sys.process._
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}
import scala.concurrent.{Future, Await, Promise, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

//java imports
import java.util.Date
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

//aws imports
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride
import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model._
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
      try {
        // This might throw an exception if the local DNS doesn't know about the system hostname.
        // At this point we're looking for some kind of identifier. It doesn't have to actually
        // be reachable.
        java.net.InetAddress.getLocalHost.getHostName
      } catch {
        case e:Exception =>
          // If all else fails..
          "hostname".!!
      }
  }

  val workerTable = new FulfillmentWorkerTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = FulfillmentWorker.this.dynamoAdapter
    def splog = FulfillmentWorker.this.splog
  }

  val taskResolutions = new mutable.Queue[TaskResolution]()

  val entry = new FulfillmentWorkerEntry
  entry.tableName = workerTable.dynamoAdapter.config.getString("worker_status_table")
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


    val getch = new Getch
    val doneFuture = setupQuitKey(getch)
    getch.doWith {
      handleTaskFuture(swfAdapter.getTask)
      Await.result(doneFuture, 1000 seconds )
    }
    shutdown
  }

  def setupQuitKey(getch: Getch): Future[Boolean] = {
    var donePromise = Promise[Boolean]()
    getch.addMapping(
      Seq("q", "Q", "Exit"), () => {
        //stdin received quit request. Respond on stdout
        println("Quitting...")
        updateStatus("Terminated by user", "WARN")
        donePromise.success(true)
      }
    )
    //echo a dot
    getch.addMapping(Seq("."), () => { print(".") } )
    donePromise.future
  }

  def shutdown() = {
    updateStatus("Exiting")
    val excsvc = swfAdapter.client.getExecutorService()
    excsvc.shutdown
    if (!excsvc.awaitTermination(3, TimeUnit.SECONDS)) {
      val tasks = excsvc.shutdownNow()
      splog.warning("Performing hard shutdown with remaining tasks: " + tasks.length)
      Thread.sleep(100)
      swfAdapter.client.shutdown
    }
  }

  def handleTaskFuture(taskFuture: Future[Option[ActivityTask]]):Unit = {
    taskFuture onComplete {
      case Success(taskopt) =>
        taskopt match {
          case Some(task) =>
            try {
              _lastTaskToken = task.getTaskToken
                val shortToken = (_lastTaskToken takeRight 10)
                updateStatus("Processing task.." + shortToken )
                handleTask(getSpecification.getParameters(task.getInput))
            } catch {
              case e:Exception =>
                failTask(s"""{"$name": "${e.toString}"}""", e.getMessage)
            }
          case None =>
            splog.info("no task available")
        }
        //get the next one
        handleTaskFuture(swfAdapter.getTask)
      case Failure(e) =>
        val ms = 1000
        updateStatus(s"Polling Exception ${e.getMessage}: waiting ${ms}ms before retrying", "EXCEPTION")
        Thread.sleep(ms)
        handleTaskFuture(swfAdapter.getTask)
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

class ActivitySpecification(val params:List[ActivityParameter], val result:ActivityResult, val description:String = "") {

  val crypter = new Crypter("config/crypto")
  val paramsMap:Map[String,ActivityParameter] = (for(param <- params) yield param.name -> param).toMap

  def getSpecification:JsValue = {
    Json.toJson(Map(
      "parameters" -> Json.toJson((for(param <- params) yield param.name -> param.toJson).toMap),
      "result" -> result.toJson,
      "description" -> Json.toJson(description)
    ))
  }

  override def toString:String = {
    Json.stringify(getSpecification)
  }

  def getParameters(input:String):ActivityParameters = {
    getParameters(Json.parse(input).as[JsObject])
  }

  def getParameters(inputObject:JsObject):ActivityParameters = {
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
      ,Json.stringify(inputObject))
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

  def getOrElse(param:String, default:Int):Int = {
    if(has(param)) {
      try {
        params(param).toInt
      } catch {
        case _: Exception => default
      }
    } else {
        default
    }
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
  this: DynamoAdapterComponent
    with SploggerComponent =>

  waitForActiveTable()

  def waitForActiveTable() = {

    var active = false
    while(!active) {
      try {
        splog.info(s"Checking for worker status table ${dynamoAdapter.tableName}")
        val tableDesc = dynamoAdapter.client.describeTable(dynamoAdapter.tableName)

        // I didn't see any constants for these statuses..
        tableDesc.getTable.getTableStatus match {
          case "CREATING" =>
            splog.info("Worker status table is being created. Let's wait a while")
            Thread.sleep(5000)
          case "UPDATING" =>
            splog.info("Worker status table is being updated. Let's wait a while")
            Thread.sleep(5000)
          case "DELETING" =>
            val errstr = "The worker status table is being deleted!"
            splog.error(errstr)
            throw new Exception(s"ERROR! $errstr")
          case "ACTIVE" =>
            splog.info("Worker status table is active")
            active = true
        }
      } catch {
        case rnfe:ResourceNotFoundException =>
          splog.warning(s"Table not found! Creating it!")
          createWorkerTable()
      }
    }
  }

  def createWorkerTable() = {
    val ctr = new CreateTableRequest()
    ctr.setTableName(dynamoAdapter.tableName)
    ctr.setProvisionedThroughput(new ProvisionedThroughput(dynamoAdapter.readCapacity, dynamoAdapter.writeCapacity))
    ctr.setAttributeDefinitions(List(new AttributeDefinition("instance", "S")))
    ctr.setKeySchema(List( new KeySchemaElement("instance", "HASH")))
    try {
      dynamoAdapter.client.createTable(ctr)
    } catch {
      case e:Exception =>
        splog.error("Error creating worker table: " + e.getMessage)
    }
  }

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

    val list = dynamoAdapter.mapper.scan(classOf[FulfillmentWorkerEntry], scanExp,
      new DynamoDBMapperConfig(new TableNameOverride(dynamoAdapter.tableName)))
    for(worker:FulfillmentWorkerEntry <- list) {
      worker.minutesSinceLast = Minutes.minutesBetween(DateTime.now, new DateTime(worker.last)).getMinutes
    }

    list.toList
  }

}

@DynamoDBTable(tableName="_CONFIGURED_IN_WORKER_PROPERTIES_")
class FulfillmentWorkerEntry() {
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

  var tableName:String = "_MUST_BE_SET_"

  def toJson:JsValue = {
    Json.obj(
      "instance" -> instance,
      "hostAddress" -> hostAddress,
      "domain" -> domain,
      "activityName" -> activityName,
      "activityVersion" -> activityVersion,
      "specification" -> specification,
      "status" -> status,
      "resolutionHistory" -> resolutionHistory,
      "start" -> start,
      "last" -> last,
      "minutesSinceLast" -> minutesSinceLast
    )

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
    new DynamoItem(tableName)
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
    new DynamoUpdate(tableName)
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
    }
    catch {
      case e:Exception =>
        splog.exception(e.getMessage)
      case t:Throwable =>
        splog.error(t.getMessage)
    }
    splog.info(s"Terminated $name")
  }
}

