package com.balihoo.fulfillment.deciders

import java.util.UUID.randomUUID

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride
import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model._
import com.balihoo.fulfillment.SWFHistoryConvertor
import org.joda.time.{Minutes, DateTime}
import org.keyczar.Crypter

import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.mutable

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._
import com.balihoo.fulfillment.util._

object Constants {
  final val delimiter = "##"
}

abstract class AbstractFulfillmentCoordinator {
  this: SploggerComponent
  with SWFAdapterComponent
  with DynamoAdapterComponent =>

  val instanceId = randomUUID().toString

  //can't have constructor code using the self type reference
  // unless it was declared 'lazy'. If not, swfAdapter is still null
  // and will throw a NullPointerException at this time.
  val domain = new SWFName(swfAdapter.config.getString("domain"))
  val workflowName = new SWFName(swfAdapter.config.getString("workflowName"))
  val workflowVersion = new SWFVersion(swfAdapter.config.getString("workflowVersion"))
  val taskListName = new SWFName(workflowName + workflowVersion)

  val taskList: TaskList = new TaskList()
    .withName(taskListName)

  val taskReq: PollForDecisionTaskRequest = new PollForDecisionTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  val coordinatorTable = new FulfillmentCoordinatorTable with DynamoAdapterComponent with SploggerComponent {
    def dynamoAdapter = AbstractFulfillmentCoordinator.this.dynamoAdapter
    def splog = AbstractFulfillmentCoordinator.this.splog
  }

// TODO Implement me!
//  val taskResolutions = new mutable.Queue[TaskResolution]()

  val entry = new FulfillmentCoordinatorEntry
  entry.tableName = coordinatorTable.dynamoAdapter.config.getString("coordinator_status_table")
  entry.setInstance(instanceId)
  entry.setHostAddress(HostIdentity.getHostAddress)
  entry.setWorkflowName(workflowName)
  entry.setWorkflowVersion(workflowVersion)
  entry.setSpecification("[]")
  entry.setDomain(domain)
  entry.setStatus("--")
  entry.setResolutionHistory("[]")
  entry.setStart(UTCFormatter.format(DateTime.now))

  def coordinate() = {

    splog.info(s"$domain $taskListName")

    declareCoordinator()

    var done = false
    val getch = new Getch
    getch.addMapping(Seq("quit", "Quit", "exit", "Exit"), () => {splog.info("\nExiting...\n");done = true})
    getch.addMapping(Seq("ping"), () => { println("pong") } )

    getch.doWith {
      while(!done) {
        try {
          updateStatus("Polling")
          val task: DecisionTask = swfAdapter.client.pollForDecisionTask(taskReq)

          if(task.getTaskToken != null) {

            updateStatus("Processing "+ task.getTaskToken takeRight 12)
            splog.info(s"processing token ${task.getTaskToken}")
            val sections = new Fulfillment(SWFHistoryConvertor.historyToSWFEvents(task.getEvents))
            val decisions = new DecisionGenerator(sections).makeDecisions()

            val response: RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest
            response.setTaskToken(task.getTaskToken)
            response.setDecisions(asJavaCollection(decisions))
            swfAdapter.client.respondDecisionTaskCompleted(response)
          }
        } catch {
          case se: java.net.SocketException =>
          // these happen.. no biggie.
          case e: Exception =>
            splog.error(e.getMessage)
        }
      }
    }
    splog.info("Done. Cleaning up...")
  }

  def declareCoordinator() = {
    val status = s"Declaring $domain $taskListName"
    splog("INFO",status)
    entry.setLast(UTCFormatter.format(DateTime.now))
    entry.setStatus(status)
    coordinatorTable.insert(entry)
  }

  def updateStatus(status:String, level:String="INFO") = {
    try {
      entry.setLast(UTCFormatter.format(DateTime.now))
      entry.setStatus(status)
      coordinatorTable.update(entry)
      splog(level,status)
    } catch {
      case e:Exception =>
        //splog will print to stdout on any throwable, or log to the default logfile
        splog("ERROR", s"Failed to update status: ${e.toString}")
    }
  }
}


class OperatorResult(val rtype:String, description:String, val sensitive:Boolean = false) {
  def toJson:JsValue = {
    Json.obj(
      "type" -> rtype,
      "description" -> description,
      "sensitive" -> sensitive
    )
  }
}

class OperatorParameter(val name:String, val ptype:String, val description:String, val required:Boolean = true, val sensitive:Boolean = false) {
  var value:Option[String] = None

  def toJson:JsValue = {
    Json.obj(
      "name" -> name,
      "type" -> ptype,
      "description" -> description,
      "required" -> required,
      "sensitive" -> sensitive
    )
  }
}

class OperatorSpecification(val params:List[OperatorParameter], val result:OperatorResult) {

  val crypter = new Crypter("config/crypto")
  val paramsMap:Map[String,OperatorParameter] = (for(param <- params) yield param.name -> param).toMap

  def toJson:JsValue = {
    Json.obj(
      "parameters" -> Json.toJson((for(param <- params) yield param.name -> param.toJson).toMap),
      "result" -> result.toJson
    )
  }

  override def toString:String = {
    Json.stringify(toJson)
  }

  def getParameters(inputParams:Map[String, String]):OperatorParameters = {
    val outputMap = mutable.Map[String, String]()
    for((name, value) <- inputParams) {
      if(paramsMap contains name) {
        val param = paramsMap(name)
        param.value = Some(
          if(param.sensitive)
            crypter.decrypt(value)
          else
            value
        )
      } else {
        outputMap(name) = value
      }
    }
    for(param <- params) {
      if(param.required && param.value.isEmpty) {
        throw new Exception(s"input parameter '${param.name}' is REQUIRED!")
      }
    }

    new OperatorParameters(
      (for((name, param) <- paramsMap if param.value.isDefined) yield param.name -> param.value.get).toMap ++ outputMap.toMap
      ,Json.stringify(Json.toJson(inputParams)))
  }

}

class OperatorParameters(val params:Map[String,String], val input:String = "{}") {

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

/**
 *
 * @param fulfillment Fulfillment
 */
class DecisionGenerator(fulfillment: Fulfillment) {

  protected def _createTimerDecision(name:String, delaySeconds:Int, status:String, reason:String) = {

    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.StartTimer)

    val timerParams = mutable.Map[String, String]()
    timerParams("section") = name
    timerParams("status") = status
    timerParams("reason") = reason

    val attribs: StartTimerDecisionAttributes = new StartTimerDecisionAttributes
    attribs.setTimerId(randomUUID().toString)
    attribs.setStartToFireTimeout(delaySeconds.toString)
    attribs.setControl(Json.stringify(Json.toJson(timerParams.toMap)))

    decision.setStartTimerDecisionAttributes(attribs)

    decision
  }

  def _checkComplete(): Option[Decision] = {

    if(fulfillment.categorized.workComplete()) {
      // If we're done then let's just bail here
      fulfillment.timeline.success("Workflow Complete!!!", None)

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.CompleteWorkflowExecution)
      return Some(decision)
    }

    None
  }

  def _checkFailed(): Option[Decision] = {

    val failReasons = mutable.MutableList[String]()

    for(section <- fulfillment.categorized.impossible) {
      if(section.essential || fulfillment.categorized.essentialTotal == 0) {
        val message = s"Section ${section.name} is IMPOSSIBLE!"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    // Loop through the problem sections
    for(section <- fulfillment.categorized.terminal) {
      if(section.essential || fulfillment.categorized.essentialTotal == 0) {
        val message = s"Section ${section.name} is TERMINAL!"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    // Any fail reasons are non-recoverable and ultimately terminal for the workflow. We're going to end it.
    if(failReasons.length > 0) {

      val details: String = failReasons.mkString("\n\t", "\n\t", "\n")
      fulfillment.timeline.error("Workflow FAILED "+details, Some(DateTime.now))
      fulfillment.status = FulfillmentStatus.FAILED

      // TODO. We should cancel the in-progress sections as BEST as we can
      val attribs: FailWorkflowExecutionDecisionAttributes = new FailWorkflowExecutionDecisionAttributes
      attribs.setReason("There are failed sections!")
      attribs.setDetails(details)

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.FailWorkflowExecution)
      decision.setFailWorkflowExecutionDecisionAttributes(attribs)

      return Some(decision)
    }

    None
  }

  /**
   * This route is taken if the workflow receives a cancelation request.
   * We can use this notification to cleanly stop processing.
   * @return
   */
  def _checkCancelRequested():Option[Decision] = {
    if(fulfillment.status != FulfillmentStatus.CANCEL_REQUESTED) { return None }

    val attribs = new CancelWorkflowExecutionDecisionAttributes
    attribs.setDetails("Cancel Requested. Shutting down.")

    val decision = new Decision
    decision.setDecisionType(DecisionType.CancelWorkflowExecution)
    decision.setCancelWorkflowExecutionDecisionAttributes(attribs)

    Some(decision)
  }

  def makeDecisions(): List[Decision] = {

    if(fulfillment.terminal()) {
      fulfillment.timeline.error(s"Workflow is TERMINAL (${fulfillment.status})", None)
      return List()
    }

    val decisions = new mutable.MutableList[Decision]()

    do {

      fulfillment.categorized.categorize()
      decisions.clear()

      _checkCancelRequested() match {
        case d:Some[Decision] => return List(d.get)
        case _ =>
      }

      _checkComplete() match {
        case d:Some[Decision] => return List(d.get)
        case _ =>
      }

      _checkFailed() match {
        case d:Some[Decision] => return List(d.get)
        case _ =>
      }

      for(section <- fulfillment.categorized.ready) {
        val delaySeconds = section.calculateWaitSeconds()
        // Does this task need to be delayed until the waitUntil time?
        if (delaySeconds > 0) {
          decisions += waitDecision(section, delaySeconds)
        } else if(section.action.isDefined) {
          decisions += _createActivityDecision(section)
        }
      }

    } while(fulfillment.categorized.checkPromoted)

    fulfillment.categorized.categorize()

    // Loop through the problem sections
    for(section <- fulfillment.categorized.failed) {
      val message = s"Section failed and is allowed to retry (${section.failedCount} of ${section.failureParams.maxRetries})"
      section.timeline.warning(message, None)
      if(!section.fixable) {
        decisions += _createTimerDecision(section.name, section.failureParams.delaySeconds, SectionStatus.READY.toString,
          message)
      } else {
        section.timeline.warning("Section is marked FIXABLE.", None)
      }
    }

    for(section <- fulfillment.categorized.timedout) {
      val message = s"Section timed out and is allowed to retry (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
      section.timeline.warning(message, None)
      decisions += _createTimerDecision(section.name, section.timeoutParams.delaySeconds, SectionStatus.READY.toString,
        message)
    }

    for(section <- fulfillment.categorized.canceled) {
      val message = s"Section was canceled and is allowed to retry (${section.canceledCount} of ${section.cancelationParams.maxRetries})"
      section.timeline.warning(message, None)
      decisions += _createTimerDecision(section.name, section.cancelationParams.delaySeconds, SectionStatus.READY.toString,
        message)
    }


    if(decisions.length == 0 && !fulfillment.categorized.hasPendingSections) {

      // We aren't making any progress...
      fulfillment.status = FulfillmentStatus.BLOCKED

      if(fulfillment.categorized.terminal.length > 0) {
        fulfillment.timeline.error(
          (for(section <- fulfillment.categorized.terminal) yield section.name)
            .mkString("Terminal Sections:\n\t", "\n\t", ""), None)
      }

      if(fulfillment.categorized.blocked.length > 0) {
        fulfillment.timeline.error(
          (for(section <- fulfillment.categorized.blocked) yield section.name)
            .mkString("Blocked Sections:\n\t", "\n\t", ""), None)
      }

    }

    decisions.toList
  }

  /**
   * Creates a timer decision to delay the execution of the worker.
   * @param section the section describing the activity
   * @param waitSeconds the number of seconds the timer should wait
   * @return the decision
   */
  private def waitDecision(section: FulfillmentSection, waitSeconds: Int): Decision = {
    val message = s"Deferred until ${section.waitUntil.get}"
    _createTimerDecision(section.name, waitSeconds, SectionStatus.READY.toString, message)
  }

  /**
   * Creates a decision to trigger a worker.
   * @param section the section describing the activity
   * @return the decision
   */
  private def _createActivityDecision(section: FulfillmentSection): Decision = {
    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.ScheduleActivityTask)

    val taskList = new TaskList
    taskList.setName(section.action.get.getName+section.action.get.getVersion)

    val attribs: ScheduleActivityTaskDecisionAttributes = new ScheduleActivityTaskDecisionAttributes
    attribs.setActivityType(section.action.get)
    attribs.setInput(Json.stringify(Json.toJson(section.gatherParameters())))
    attribs.setTaskList(taskList)
    attribs.setActivityId(section.getActivityId)

    if(section.startToCloseTimeout.isDefined) attribs.setStartToCloseTimeout(section.startToCloseTimeout.get)
    if(section.scheduleToStartTimeout.isDefined) attribs.setScheduleToStartTimeout(section.scheduleToStartTimeout.get)
    if(section.scheduleToCloseTimeout.isDefined) attribs.setScheduleToCloseTimeout(section.scheduleToCloseTimeout.get)
    if(section.heartbeatTimeout.isDefined) attribs.setHeartbeatTimeout(section.heartbeatTimeout.get)

    decision.setScheduleActivityTaskDecisionAttributes(attribs)

    fulfillment.timeline.note("Scheduling work for: "+section.name, None)

    decision
  }
}

class FulfillmentCoordinatorTable {
  this: DynamoAdapterComponent
    with SploggerComponent =>

  val tableName = dynamoAdapter.config.getString("coordinator_status_table")
  val readCapacity = dynamoAdapter.config.getOrElse("coordinator_status_read_capacity", 3)
  val writeCapacity = dynamoAdapter.config.getOrElse("coordinator_status_write_capacity", 5)

  waitForActiveTable()

  def waitForActiveTable() = {

    var active = false
    while(!active) {
      try {
        splog.info(s"Checking for coordinator status table $tableName")
        val tableDesc = dynamoAdapter.client.describeTable(tableName)

        // I didn't see any constants for these statuses..
        tableDesc.getTable.getTableStatus match {
          case "CREATING" =>
            splog.info("Coordinator status table is being created. Let's wait a while")
            Thread.sleep(5000)
          case "UPDATING" =>
            splog.info("Coordinator status table is being updated. Let's wait a while")
            Thread.sleep(5000)
          case "DELETING" =>
            val errstr = "The coordinator status table is being deleted!"
            splog.error(errstr)
            throw new Exception(s"ERROR! $errstr")
          case "ACTIVE" =>
            splog.info("Coordinator status table is active")
            active = true
        }
      } catch {
        case rnfe:ResourceNotFoundException =>
          splog.warning(s"Table not found! Creating it!")
          createCoordinatorTable()
      }
    }
  }

  def createCoordinatorTable() = {
    val ctr = new CreateTableRequest()
    ctr.setTableName(tableName)
    ctr.setProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity))
    ctr.setAttributeDefinitions(List(new AttributeDefinition("instance", "S")))
    ctr.setKeySchema(List( new KeySchemaElement("instance", "HASH")))
    try {
      dynamoAdapter.client.createTable(ctr)
    } catch {
      case e:Exception =>
        splog.error("Error creating coordinator table: " + e.getMessage)
    }
  }

  def insert(entry:FulfillmentCoordinatorEntry) = {
    dynamoAdapter.put(entry.getDynamoItem)
  }

  def update(entry:FulfillmentCoordinatorEntry) = {
    dynamoAdapter.update(entry.getDynamoUpdate)
  }

  def get() = {
    val scanExp:DynamoDBScanExpression = new DynamoDBScanExpression()

    val oldest = DateTime.now.minusDays(1)

    scanExp.addFilterCondition("last",
      new Condition()
        .withComparisonOperator(ComparisonOperator.GT)
        .withAttributeValueList(new AttributeValue().withS(UTCFormatter.format(oldest))))

    val list = dynamoAdapter.mapper.scan(classOf[FulfillmentCoordinatorEntry], scanExp,
      new DynamoDBMapperConfig(new TableNameOverride(tableName)))
    for(coordinator:FulfillmentCoordinatorEntry <- list) {
      coordinator.minutesSinceLast = Minutes.minutesBetween(DateTime.now, new DateTime(coordinator.last)).getMinutes
    }

    list.toList
  }

}

@DynamoDBTable(tableName="_CONFIGURED_IN_COORDINATOR_PROPERTIES_")
class FulfillmentCoordinatorEntry() {
  var instance:String = ""
  var hostAddress:String = ""
  var domain:String = ""
  var workflowName:String = ""
  var workflowVersion:String = ""
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
      "workflowName" -> workflowName,
      "workflowVersion" -> workflowVersion,
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

  @DynamoDBAttribute(attributeName="workflowName")
  def getWorkflowName:String = { workflowName }
  def setWorkflowName(workflowName:String) { this.workflowName = workflowName; }

  @DynamoDBAttribute(attributeName="workflowVersion")
  def getWorkflowVersion:String = { workflowVersion }
  def setWorkflowVersion(workflowVersion:String) { this.workflowVersion = workflowVersion; }

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
      .addString("workflowName", workflowName)
      .addString("workflowVersion", workflowVersion)
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

class FulfillmentCoordinator(swf: SWFAdapter, dyn:DynamoAdapter, splogger: Splogger)
  extends AbstractFulfillmentCoordinator
  with SploggerComponent
  with SWFAdapterComponent
  with DynamoAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def splog = splogger
}

object coordinator {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog.info(s"Started $name")
    try {
      val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
      splog.debug("Created PropertiesLoader")
      val swf = new SWFAdapter(config, splog, true)
      splog.debug("Created SWFAdapter")
      val dyn = new DynamoAdapter(config)
      splog.debug("Created DynamoAdapter")
      val fc = new FulfillmentCoordinator(swf, dyn, splog)
      splog.debug("Created FulfillmentCoordinator")
      fc.coordinate()
    }
    catch {
      case e:Exception =>
        splog.error(e.getMessage)
    }
    splog("INFO", s"Terminated $name")
  }
}
