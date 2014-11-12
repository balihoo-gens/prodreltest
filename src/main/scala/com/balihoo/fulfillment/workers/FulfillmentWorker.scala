package com.balihoo.fulfillment.workers

//scala imports

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}

import scala.collection.mutable
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}
import scala.concurrent.{Future, Await, Promise, ExecutionContext}
import scala.concurrent.duration._

//java imports
import java.util.UUID.randomUUID
import java.util.concurrent.{TimeUnit, Executors}

//aws imports
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride
import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.simpleworkflow.model._

//play imports
import play.api.libs.json._

//other external
import org.joda.time.{Minutes, DateTime}
import org.keyczar.Crypter

//local imports
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util._

abstract class FulfillmentWorker {
  this: LoggingWorkflowAdapter =>

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  val instanceId = randomUUID().toString

  def domain = swfAdapter.domain
  def name = swfAdapter.name
  def version = swfAdapter.version
  def taskListName = swfAdapter.taskListName
  def taskList = swfAdapter.taskList

  val crypter = new Crypter("config/crypto")

  val defaultTaskHeartbeatTimeout = swfAdapter.config.getString("default_task_heartbeat_timeout")
  val defaultTaskScheduleToCloseTimeout = swfAdapter.config.getString("default_task_schedule_to_close_timeout")
  val defaultTaskScheduleToStartTimeout = swfAdapter.config.getString("default_task_schedule_to_start_timeout")
  val defaultTaskStartToCloseTimeout = swfAdapter.config.getString("default_task_start_to_close_timeout")

  val workerTable = new FulfillmentWorkerTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = FulfillmentWorker.this.dynamoAdapter
    def splog = FulfillmentWorker.this.splog
  }

  val taskResolutions = new mutable.Queue[TaskResolution]()

  val entry = new FulfillmentWorkerEntry
  entry.tableName = workerTable.dynamoAdapter.config.getString("worker_status_table")
  entry.setInstance(instanceId)
  entry.setHostAddress(HostIdentity.getHostAddress)
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
  var _doneFuture: Option[Future[Boolean]] = None

  def work() = {

    registerActivityType()

    updateStatus("Starting")

    val getch = new Getch
    _doneFuture = Some(setupQuitKey(getch))
    getch.doWith {
      handleTaskFuture(swfAdapter.getTask)
      Await.result(_doneFuture.get, Duration.Inf )
    }
    shutdown()
  }

  def setupQuitKey(getch: Getch): Future[Boolean] = {
    val donePromise = Promise[Boolean]()
    getch.addMapping(
      Seq("quit", "Quit", "exit", "Exit"), () => {
        //stdin received quit request. Respond on stdout
        println("Quitting...")
        updateStatus("Terminated by user", "WARN")
        donePromise.success(true)
      }
    )
    //respond to ping
    getch.addMapping(Seq("ping"), () => { println("pong") } )

    //return the future that will succeed when termination is requested
    donePromise.future
  }

  def shutdown() = {
    updateStatus("Exiting")
    val excsvc = swfAdapter.client.getExecutorService
    excsvc.shutdown()
    if (!excsvc.awaitTermination(3, TimeUnit.SECONDS)) {
      val tasks = excsvc.shutdownNow()
      splog.warning("Performing hard shutdown with remaining tasks: " + tasks.length)
      Thread.sleep(100)
      swfAdapter.client.shutdown()
    }
  }

  def handleTaskFuture(taskFuture: Future[Option[ActivityTask]]):Unit = {
    updateStatus("Polling")
    taskFuture onComplete {
      case Success(taskopt) =>
        taskopt match {
          case Some(task) =>
            try {
              _lastTaskToken = task.getTaskToken
              val shortToken = _lastTaskToken takeRight 10
              updateStatus("Processing task.." + shortToken )
              handleTask(getSpecification.getParameters(task.getInput))
            } catch {
              case e:Exception =>
                failTask(s"""{"$name": "${e.toString}"}""", e.getMessage)
            }
          case None =>
            splog.info("no task available")
        }

        if (_doneFuture.nonEmpty || _doneFuture.get.isCompleted) {
          //get the next one
          handleTaskFuture(swfAdapter.getTask)
        } else {
          updateStatus("Termination requested; not getting a new task", "WARNING")
        }

      //this is unusual; when no task is available, success is returned above with None
      case Failure(e) =>
        val ms = 1000
        updateStatus(s"Polling Exception ${e.getMessage}: waiting ${ms}ms before retrying", "EXCEPTION")
        if (_doneFuture.nonEmpty || _doneFuture.get.isCompleted) {
          Thread.sleep(ms)
          //get the next one
          handleTaskFuture(swfAdapter.getTask)
        } else {
          updateStatus("Termination requested; not getting a new task", "WARNING")
        }
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
    try {
      if (_lastTaskToken != null && _lastTaskToken.nonEmpty) {
        val response:RespondActivityTaskCompletedRequest = new RespondActivityTaskCompletedRequest
        response.setTaskToken(_lastTaskToken)
        response.setResult(getSpecification.result.sensitive match {
          case true => crypter.encrypt(result)
          case _ => result
        })
        swfAdapter.client.respondActivityTaskCompleted(response)
      } else {
        throw new Exception("empty task token")
      }
    } catch {
      case e:Exception =>
        splog.error(s"error completing task: ${e.getMessage}")
    }
    _resolveTask(new TaskResolution("Completed", result))
  }

  def failTask(reason:String, details:String) = {
    failedTasks += 1
    try {
      if (_lastTaskToken != null && _lastTaskToken.nonEmpty) {
        val response:RespondActivityTaskFailedRequest = new RespondActivityTaskFailedRequest
        response.setTaskToken(_lastTaskToken)
        response.setReason(reason)
        response.setDetails(details)
        swfAdapter.client.respondActivityTaskFailed(response)
      } else {
        throw new Exception("empty task token")
      }
    } catch {
      case e:Exception =>
        splog.error(s"error failing task: ${e.getMessage}")
    }
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

  def toSchema:JsValue = {
    Json.obj(
      "type" -> "object",
      "properties" -> Json.obj(
        "type" -> rtype,
        "description" -> Json.obj("type" -> "string"),
        "sensitive" -> Json.obj("type" -> "string")
      )
    )
  }
}

abstract class ActivityParameter(val name:String
                                ,val description:String
                                ,val required:Boolean = true
                                ,val sensitive:Boolean = false) {

  val crypter = new Crypter("config/crypto")
  def jsonType:String

  def parseValue(js:JsValue):Any

  def toJson:JsValue = {
    Json.toJson(Map(
      "name" -> Json.toJson(name),
      "type" -> Json.toJson(jsonType),
      "description" -> Json.toJson(description),
      "required" -> Json.toJson(required),
      "sensitive" -> Json.toJson(sensitive)
    ))
  }

  def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> s"$description sensitive=($sensitive)"
    )
  }

  protected def _parseBasic[T: Reads](js:JsValue):Any = {
    js.validate[T] match {
      case s:JsSuccess[T] =>
        Some(s.get)
      case _ =>
        throw new Exception(s"Expected $jsonType but got '${Json.stringify(js)}'!")
    }
  }

}

class StringActivityParameter(override val name:String
                             ,override val description:String
                             ,override val required:Boolean = true
                             ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "string"

  def parseValue(js:JsValue):Any = {
    if(sensitive)
      crypter.decrypt(js.as[String])
    else
      js.as[String]
  }
}

class IntegerActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true
                              ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "integer"

  def parseValue(js:JsValue):Any = _parseBasic[Long](js)
}

class NumberActivityParameter(override val name:String
                               ,override val description:String
                               ,override val required:Boolean = true
                               ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "number"

  def parseValue(js:JsValue):Any = _parseBasic[Double](js)
}

class BooleanActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true
                              ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "boolean"

  def parseValue(js:JsValue):Any = _parseBasic[Boolean](js)
}

class StringsActivityParameter(override val name:String
                              ,override val description:String
                              ,override val required:Boolean = true
                              ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "array"

  def parseValue(js:JsValue):Any = _parseBasic[List[String]](js)

  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> s"$description sensitive=($sensitive)",
      "items" -> Json.obj(
        "type" -> "string"
      )
    )
  }
}

class ObjectActivityParameter(override val name:String
                               ,override val description:String
                               ,override val required:Boolean = true
                               ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "object"

  def parseValue(js:JsValue):Any = js.as[JsObject]

  override def toSchema:JsValue = {
    Json.obj(
      "type" -> jsonType,
      "description" -> s"$description sensitive=($sensitive)",
      "properties" -> Json.obj()
    )
  }
}

class EnumActivityParameter(override val name:String
                               ,override val description:String
                               ,val options:Seq[String]
                               ,override val required:Boolean = true
                               ,override val sensitive:Boolean = false)
  extends ActivityParameter(name, description, required, sensitive) {

  def jsonType = "enum"

  def parseValue(js:JsValue):Any = _parseBasic[String](js)

  override def toSchema:JsValue = {
    Json.obj(
      "enum" -> Json.toJson(options)
    )
  }
}

class ActivitySpecification(val params:List[ActivityParameter]
                           ,val result:ActivityResult
                           ,val description:String = "") {

  val paramsMap:Map[String,ActivityParameter] = (for(param <- params) yield param.name -> param).toMap

  private val __factory = JsonSchemaFactory.byDefault()
  private val __schema:JsonSchema = __factory.getJsonSchema(parameterSchema.as[JsonNode])

  def getSpecification:JsValue = {
    Json.obj(
      "parameters" -> Json.toJson((for(param <- params) yield param.name -> param.toJson).toMap),
      "result" -> result.toJson,
      "description" -> Json.toJson(description),
      "schema" -> parameterSchema
    )
  }

  override def toString:String = {
    Json.stringify(getSpecification)
  }

  def parameterSchema:JsObject = {
    Json.obj(
      "$schema" ->"http://json-schema.org/draft-04/schema", // Our preferred schema
      "type" -> "object",
      "required" -> (for((pname, param) <- paramsMap if param.required) yield pname),
      "properties" ->  (for((pname, param) <- paramsMap) yield pname -> param.toSchema)
    )
  }

  def validate(js:JsValue) = {
    val report = __schema.validate(js.as[JsonNode])

    report.isSuccess match {
      case false =>
          throw new Exception(report.toString)
//        for(m:ProcessingMessage <- report) {
//          val report = Json.toJson(m.asJson).as[JsObject]
//          report.value("domain").as[String]
//          report.value("required").as[List[String]]
          //        for((k, v) <- Json.toJson(m.asJson).as[JsObject].fields) {
          //          println(s"$k:$v")
          //        }
//        }
      case _ =>
    }
  }

  def getParameters(input:String):ActivityParameters = {
    getParameters(Json.parse(input).as[JsObject])
  }

  def getParameters(inputObject:JsObject):ActivityParameters = {
    val foundParams = mutable.Map[String, Any]()
    for((name, value) <- inputObject.fields) {
      if(paramsMap contains name) {
        val param = paramsMap(name)
        foundParams(name) = param.parseValue(value)
      }
    }
    // TODO FIXME This should be handled by the schema validation
    for(param <- params) {
      if(param.required && !(foundParams contains param.name)) {
        throw new Exception(s"input parameter '${param.name}' is REQUIRED!")
      }
    }

    new ActivityParameters(foundParams.toMap, Json.stringify(inputObject))
  }

}

class ActivityParameters(val params:Map[String,Any], val input:String = "{}") {

  def has(param:String):Boolean = {
    params contains param
  }

  def apply[T](param:String):T = {
    params(param).asInstanceOf[T]
  }

  def getOrElse[T](param:String, default:T):T = {
    if(has(param)) {
      return params(param).asInstanceOf[T]
    }
    default
  }


  def get[T](param: String): Option[T] = params.get(param).asInstanceOf[Option[T]]

  override def toString:String = {
    params.toString()
  }
}


class FulfillmentWorkerTable {
  this: DynamoAdapterComponent
    with SploggerComponent =>

  val tableName = dynamoAdapter.config.getString("worker_status_table")
  val readCapacity = dynamoAdapter.config.getOrElse("worker_status_read_capacity", 3)
  val writeCapacity = dynamoAdapter.config.getOrElse("worker_status_write_capacity", 5)

  waitForActiveTable()

  def waitForActiveTable() = {

    var active = false
    while(!active) {
      try {
        splog.info(s"Checking for worker status table $tableName")
        val tableDesc = dynamoAdapter.client.describeTable(tableName)

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
    ctr.setTableName(tableName)
    ctr.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity))
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
      new DynamoDBMapperConfig(new TableNameOverride(tableName)))
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
    }
    splog.info(s"Terminated $name")
  }
}

