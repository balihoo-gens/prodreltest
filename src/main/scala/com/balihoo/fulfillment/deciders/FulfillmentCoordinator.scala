package com.balihoo.fulfillment.deciders

import java.security.MessageDigest
import java.util.UUID.randomUUID

import org.joda.time.DateTime
import org.keyczar.Crypter

import scala.language.implicitConversions
import scala.collection.convert.wrapAsJava._
import scala.collection.mutable

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._
import com.balihoo.fulfillment.util.{Getch, Splogger, SploggerComponent}

import scala.util.matching.Regex

object Constants {
  final val delimiter = "##"
}

abstract class AbstractFulfillmentCoordinator {
  this: SploggerComponent
  with SWFAdapterComponent =>

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

  val operators = new FulfillmentOperators

  def coordinate() = {

    splog.info(s"$domain $taskListName")

    var done = false
    val getch = new Getch
    getch.addMapping(Seq("q", "Q", "Exit"), () => {splog.info("\nExiting...\n");done = true})

    getch.doWith {
      while(!done) {
        try {
          val task: DecisionTask = swfAdapter.client.pollForDecisionTask(taskReq)

          if(task.getTaskToken != null) {

            splog.info(s"processing token ${task.getTaskToken.toString}")
            val sections = new FulfillmentSections(task.getEvents)
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
          case t: Throwable =>
            splog.error(t.getMessage)
        }
      }
    }
    splog.info("Done. Cleaning up...")
  }
}


class OperatorResult(val rtype:String, description:String, val sensitive:Boolean = false) {
  def toJson:JsValue = {
    Json.toJson(Map(
      "type" -> Json.toJson(rtype),
      "description" -> Json.toJson(description),
      "sensitive" -> Json.toJson(sensitive)
    ))
  }
}

class OperatorParameter(val name:String, val ptype:String, val description:String, val required:Boolean = true, val sensitive:Boolean = false) {
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

class OperatorSpecification(val params:List[OperatorParameter], val result:OperatorResult) {

  val crypter = new Crypter("config/crypto")
  val paramsMap:Map[String,OperatorParameter] = (for(param <- params) yield param.name -> param).toMap

  def toJson:JsValue = {
    Json.toJson(Map(
      "parameters" -> Json.toJson((for(param <- params) yield param.name -> param.toJson).toMap),
      "result" -> result.toJson
    ))
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

class FulfillmentOperator(val name:String, val specification:OperatorSpecification, code:(OperatorParameters) => String) {

  def apply(inputParams:Map[String, String]):String = {
    code(specification.getParameters(inputParams))
  }
}

class FulfillmentOperators {

  var operators = mutable.Map[String, FulfillmentOperator]()

  registerOperator(
    new FulfillmentOperator(
      "MD5",
      new OperatorSpecification(
        List(new OperatorParameter("input", "string", "The string to MD5")),
        new OperatorResult("string", "MD5 checksum of 'input'")
      ),
      (parameters) => {
        MessageDigest.getInstance("MD5").digest(parameters("input").getBytes).map("%02X".format(_)).mkString
      })
  )
  registerOperator(
    new FulfillmentOperator(
      "StringFormat",
      new OperatorSpecification(
        List(new OperatorParameter("format", "string", "A string containing {param1} {param2}.. tokens")
            ,new OperatorParameter("...", "string", "Strings to be substituted into 'format' at token locations", false)
        ),
        new OperatorResult("string", "'format' with tokens substituted")
      ),
      (parameters) => {
        val pattern = new Regex("""\{(\w+)\}""", "token")
          pattern.replaceAllIn(parameters("format"),
          m => parameters.getOrElse(m.group("token"), "--"))
      })
  )

  def registerOperator(operator:FulfillmentOperator) = {
    operators(operator.name) = operator
  }

  def apply(operator:String, params:Map[String, String]):String = {
    operators(operator)(params)
  }

  def toJson:JsValue = {
    Json.toJson((for((name, operator) <- operators) yield name -> operator.specification.toJson).toMap)
  }
}

/**
 *
 * @param sections A Name -> Section mapping helper
 */
class DecisionGenerator(sections: FulfillmentSections) {

  val operators = new FulfillmentOperators

  protected def gatherParameters(section: FulfillmentSection
                                ,sections: FulfillmentSections):Map[String,String] = {

    val params = mutable.Map[String, String]()

    for((name, value) <- section.params) {
      value match {
        case sectionReferences: SectionReferences =>
          params(name) = sectionReferences.getValue(sections)
        case v: String =>
          params(name) = v
        case _ =>
          section.timeline.warning(s"Parameter '$name' doesn't have a recognizable value '$value'", None)
      }
    }

    params.toMap
  }


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

  protected def operate(section: FulfillmentSection) = {

    val params = gatherParameters(section, sections)
    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.RecordMarker)
    val attribs:RecordMarkerDecisionAttributes = new RecordMarkerDecisionAttributes
    var outcome = "SUCCESS"
    try {
      val result = operators(section.operator.get, params)
      section.setCompleted(result, DateTime.now)
      attribs.setDetails(section.value)
    } catch {
      case e:Exception =>
        outcome = "FAILURE"
        section.setFailed("Operation Failed", e.getMessage, DateTime.now)
        attribs.setDetails(e.getMessage)
    }
    attribs.setMarkerName(s"OperatorResult${Constants.delimiter}${section.name}${Constants.delimiter}$outcome")
    decision.setRecordMarkerDecisionAttributes(attribs)
    decision
  }

  def _checkComplete(): Option[Decision] = {

    if(sections.categorized.workComplete()) {
      // If we're done then let's just bail here
      sections.timeline.success("Workflow Complete!!!", None)

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.CompleteWorkflowExecution)
      return Some(decision)
    }

    None
  }

  def _checkFailed(): Option[Decision] = {

    val failReasons = mutable.MutableList[String]()

    if(sections.categorized.impossible.length > 0) {
      failReasons += (for(section <- sections.categorized.impossible) yield s"${section.name}").mkString("Impossible Sections:\n\t", ", ", "")
    }

    // Loop through the problem sections
    for(section <- sections.categorized.failed) {
      if(section.failedCount >= section.failureParams.maxRetries) {
        val message = s"Section $section FAILED too many times! (${section.failedCount} of ${section.failureParams.maxRetries})"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    for(section <- sections.categorized.timedout) {
      if(section.timedoutCount >= section.timeoutParams.maxRetries) {
        val message =  s"Section $section TIMED OUT too many times! (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    for(section <- sections.categorized.canceled) {
      if(section.canceledCount >= section.cancelationParams.maxRetries) {
        val message = s"Section $section was CANCELED too many times! (${section.canceledCount} of ${section.cancelationParams.maxRetries})"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    // Any fail reasons are non-recoverable and ultimately terminal for the workflow. We're going to end it.
    if(failReasons.length > 0) {

      val details: String = failReasons.mkString("Failed Sections:\n\t", "\n\t", "\n")
      sections.timeline.error("Workflow FAILED "+details, Some(DateTime.now))

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

  def makeDecisions(runOperations:Boolean = true): List[Decision] = {

    _checkComplete() match {
      case d:Some[Decision] => return List(d.get)
      case _ =>
    }

    if(sections.terminal()) {
      sections.timeline.error(s"Workflow is TERMINAL (${sections.resolution})", None)
      return List()
    }

    _checkFailed() match {
      case d:Some[Decision] => return List(d.get)
      case _ =>
    }

    val decisions = new mutable.MutableList[Decision]()

    var decisionLength = 0

    if(runOperations) {
      do { // Loop through the operations until they're all processed
        decisionLength = decisions.length
        for(section <- sections.categorized.ready) {
          if(section.operator.isDefined) {
            decisions += operate(section)
          }
        }
        sections.categorized.categorize() // Re-categorize the sections based on the new statuses

      } while(decisions.length > decisionLength)
    }

    _checkComplete() match { // YES AGAIN.. processing any pending operations may have pushed us into complete
      case d:Some[Decision] => return decisions.toList ++ List(d.get)
      case _ =>
    }

    for(section <- sections.categorized.ready) {
      val delaySeconds = section.calculateWaitSeconds()
      // Does this task need to be delayed until the waitUntil time?
      if (delaySeconds > 0) {
        decisions += waitDecision(section, delaySeconds)
      } else if(section.action.isDefined) {
        decisions += _createActivityDecision(section)
      }
    }

    // Loop through the problem sections
    for(section <- sections.categorized.failed) {
      val message = s"Section failed and is allowed to retry (${section.failedCount} of ${section.failureParams.maxRetries})"
      section.timeline.warning(message, None)
      if(!section.fixable) {
        decisions += _createTimerDecision(section.name, section.failureParams.delaySeconds, SectionStatus.READY.toString,
          message)
      } else {
        section.timeline.warning("Section is marked FIXABLE.", None)
      }
    }

    for(section <- sections.categorized.timedout) {
      val message = s"Section timed out and is allowed to retry (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
      section.timeline.warning(message, None)
      decisions += _createTimerDecision(section.name, section.timeoutParams.delaySeconds, SectionStatus.READY.toString,
        message)
    }

    for(section <- sections.categorized.canceled) {
      val message = s"Section was canceled and is allowed to retry (${section.canceledCount} of ${section.cancelationParams.maxRetries})"
      section.timeline.warning(message, None)
      decisions += _createTimerDecision(section.name, section.cancelationParams.delaySeconds, SectionStatus.READY.toString,
        message)
    }


    if(decisions.length == 0 && !sections.categorized.hasPendingSections) {

      // We aren't making any progress...
      sections.resolution = "BLOCKED"

      if(sections.categorized.failed.length > 0) {
        sections.timeline.error(
          (for(section <- sections.categorized.failed) yield section.name)
            .mkString("Failed Sections:\n\t", "\n\t", ""), None)
      }

      if(sections.categorized.blocked.length > 0) {
        sections.timeline.error(
          (for(section <- sections.categorized.blocked) yield section.name)
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
    val params = gatherParameters(section, sections)

    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.ScheduleActivityTask)

    val taskList = new TaskList
    taskList.setName(section.action.get.getName+section.action.get.getVersion)

    val attribs: ScheduleActivityTaskDecisionAttributes = new ScheduleActivityTaskDecisionAttributes
    attribs.setActivityType(section.action.get)
    attribs.setInput(Json.stringify(Json.toJson(params)))
    attribs.setTaskList(taskList)
    attribs.setActivityId(section.getActivityId)

    if(section.startToCloseTimeout.isDefined) attribs.setStartToCloseTimeout(section.startToCloseTimeout.get)
    if(section.scheduleToStartTimeout.isDefined) attribs.setScheduleToStartTimeout(section.scheduleToStartTimeout.get)
    if(section.scheduleToCloseTimeout.isDefined) attribs.setScheduleToCloseTimeout(section.scheduleToCloseTimeout.get)
    if(section.heartbeatTimeout.isDefined) attribs.setHeartbeatTimeout(section.heartbeatTimeout.get)

    decision.setScheduleActivityTaskDecisionAttributes(attribs)

    sections.timeline.note("Scheduling work for: "+section.name, None)

    decision
  }
}

class FulfillmentCoordinator(swf: SWFAdapter, splogger: Splogger)
  extends AbstractFulfillmentCoordinator
  with SploggerComponent
  with SWFAdapterComponent {
    def swfAdapter = swf
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
      val swf = new SWFAdapter(config)
      splog.debug("Created SWFAdapter")
      val fc = new FulfillmentCoordinator(swf, splog)
      splog.debug("Created FulfillmentCoordinator")
      fc.coordinate()
    }
    catch {
      case e:Exception =>
        splog.error(e.getMessage)
      case t:Throwable =>
        splog.error(t.getMessage)
    }
    splog("INFO", s"Terminated $name")
  }
}
