package com.balihoo.fulfillment.deciders

import java.util.UUID.randomUUID

import scala.language.implicitConversions
import scala.collection.convert.wrapAsScala._
import scala.collection.convert.wrapAsJava._
import scala.collection.mutable

import com.balihoo.fulfillment.SWFAdapter

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._
import com.balihoo.fulfillment.config.PropertiesLoader

object SectionStatus extends Enumeration {
  val INCOMPLETE = Value("INCOMPLETE")
  val SCHEDULED = Value("SCHEDULED")
  val STARTED = Value("STARTED")
  val FAILED = Value("FAILED")
  val TIMED_OUT = Value("TIMED OUT")
  val CANCELED = Value("CANCELED")
  val COMPLETE = Value("COMPLETE")
  val DEFERRED = Value("DEFERRED")
  val IMPOSSIBLE = Value("IMPOSSIBLE")

}

class ActionParams(max:Int, delay:Int) {
  var maxRetries:Int = max
  var delaySeconds:Int = delay
}

class FulfillmentSection(sectionName: String
                         ,jsonNode: JsObject) {

  var name = sectionName
  var action: ActivityType = null
  val params: collection.mutable.Map[String, Any] = collection.mutable.Map[String, Any]()
  var prereqs: mutable.MutableList[String] = mutable.MutableList[String]()
  var notes: mutable.MutableList[String] = mutable.MutableList[String]()
  var value: String = ""

  var status = SectionStatus.INCOMPLETE

  var scheduledCount: Int = 0
  var startedCount: Int = 0
  var timedoutCount: Int = 0
  var canceledCount: Int = 0
  var failedCount: Int = 0

  var failureParams = new ActionParams(0, 0)
  var timeoutParams = new ActionParams(0, 0)
  var cancelationParams = new ActionParams(0, 0)

  var startToCloseTimeout = ""
  var scheduleToStartTimeout = ""
  var scheduleToCloseTimeout = ""
  var heartbeatTimeout = ""

  for((key, value) <- jsonNode.fields) {
    key match {
      case "action" =>
        val jaction = value.as[JsObject]
        action = new ActivityType
        action.setName(jaction.value("name").as[String])
        action.setVersion(jaction.value("version").as[String])

        if(jaction.keys contains "failure") {
          val failure = jaction.value("failure").as[JsObject]
          failureParams.maxRetries =  failure.value("max").as[String].toInt
          failureParams.delaySeconds =  failure.value("delay").as[String].toInt
        }
        if(jaction.keys contains "timeout") {
          val timeout = jaction.value("timeout").as[JsObject]
          timeoutParams.maxRetries =  timeout.value("max").as[String].toInt
          timeoutParams.delaySeconds =  timeout.value("delay").as[String].toInt
        }
        if(jaction.keys contains "cancelation") {
          val cancelation = jaction.value("cancelation").as[JsObject]
          cancelationParams.maxRetries =  cancelation.value("max").as[String].toInt
          cancelationParams.delaySeconds =  cancelation.value("delay").as[String].toInt
        }
        if(jaction.keys contains "startToCloseTimeout") {
          startToCloseTimeout = jaction.value("startToCloseTimeout").as[String]
        }
        if(jaction.keys contains "scheduleToCloseTimeout") {
          startToCloseTimeout = jaction.value("scheduleToCloseTimeout").as[String]
        }
        if(jaction.keys contains "scheduleToStartTimeout") {
          startToCloseTimeout = jaction.value("scheduleToStartTimeout").as[String]
        }
        if(jaction.keys contains "heartbeatTimeout") {
          startToCloseTimeout = jaction.value("heartbeatTimeout").as[String]
        }

      case "params" =>
        val jparams = value.as[JsObject]
        for((jk, jv) <- jparams.fields) {
          jv match {
            case jObj: JsObject =>
              params += (jk -> new SectionReference(jObj.value("section").as[String]))
            case jStr: JsValue =>
              params += (jk -> jv.as[String])
            case _ =>
              notes += s"Parameter type for param $jk is not understood!"
          }
        }

      case "prereqs" =>
        val jprereqs = value.as[JsArray]
        for(element <- jprereqs.as[List[String]]) {
          prereqs += element
        }

      case "status" =>
        status = SectionStatus.withName(value.as[String])

      case _ =>
        notes += s"Section $key unhandled!"
    }
  }

  def getActivityId = {
    val timestamp: Long = System.currentTimeMillis()
    s"$name-${action.getName}-"+timestamp
  }

  override def toString = {
    s"""
      |$name $status
      |  Action: $action
      |  Params: $params
      |  Prereqs: $prereqs
      |  Notes: $notes
      |  Value: $value
    """.stripMargin
  }
}

/**
 * Bin the sections by status. So we can make decisions
 * @param sections SectionMap
 */
class CategorizedSections(sections: SectionMap) {
  var complete: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var inprogress: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var timedout: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var deferred: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var blocked: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var failed: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var canceled: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var ready: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()
  var impossible: mutable.MutableList[FulfillmentSection] = mutable.MutableList[FulfillmentSection]()

  for((name, section) <- sections.map) {
    section.status match {
      case SectionStatus.COMPLETE =>
        complete += section
      case SectionStatus.SCHEDULED =>
        inprogress += section
      case SectionStatus.STARTED =>
        inprogress += section
      case SectionStatus.FAILED =>
        failed += section
      case SectionStatus.CANCELED =>
        canceled += section
      case SectionStatus.TIMED_OUT =>
        timedout += section
      case SectionStatus.DEFERRED =>
        deferred += section
      case SectionStatus.INCOMPLETE =>
        categorizeIncompleteSection(section)
      case SectionStatus.IMPOSSIBLE =>
        impossible += section
      case _ => println(section.status + " is not handled here!")
    }
  }

  /**
   * This is a special case.. incomplete sections are either 'READY' or 'BLOCKED'
   * We have to examine the params/prereqs to know if this section is runnable yet
   * @param section FulfillmentSection
   * @return
   */
  protected def categorizeIncompleteSection(section: FulfillmentSection) = {

    var paramsReady: Boolean = true
    for((name, value) <- section.params) {
      value match {
        case sectionReference: SectionReference =>
          val referencedSection: FulfillmentSection = sections.map(sectionReference.name)
          if(referencedSection.status != SectionStatus.COMPLETE) {
            section.notes += s"Waiting for parameter $name (${referencedSection.status})"
            paramsReady = false
          }
        case _ =>
      }
    }

    var prereqsReady: Boolean = true
    for(prereq: String <- section.prereqs) {
      val referencedSection: FulfillmentSection = sections.map(prereq)
      referencedSection.status match {
        case SectionStatus.COMPLETE =>
        //          println("Section is complete")
        case _ =>
          // Anything other than complete is BLOCKING our progress
          section.notes += s"Waiting for prereq $prereq (${referencedSection.status})"
          prereqsReady = false
      }
    }

    if(!paramsReady || !prereqsReady) {
      blocked += section
    } else {
      // Whoohoo! we're ready to run!
      ready += section
    }

  }

  def workComplete() : Boolean = {
    sections.map.size == complete.length
  }

  def hasPendingSections: Boolean = {
    inprogress.length + deferred.length != 0
  }

}

/**
 * Build and update FulfillmentSections from the SWF execution history
 * @param history java.util.List[HistoryEvent]
 */
class SectionMap(history: java.util.List[HistoryEvent]) {

  val registry = collection.mutable.Map[java.lang.Long, String]()
  val map = collection.mutable.Map[String, FulfillmentSection]()
  var notes: mutable.MutableList[String] = mutable.MutableList[String]()
  val timers = collection.mutable.Map[String, String]()

  notes += s"Processing ${history.last.getEventType}..."

  try {
    for(event: HistoryEvent <- collectionAsScalaIterable(history)) {
      EventType.fromValue(event.getEventType) match {
        case EventType.WorkflowExecutionStarted =>
          processWorkflowExecutionStarted(event)
        case EventType.ActivityTaskScheduled =>
          processActivityTaskScheduled(event)
        case EventType.ActivityTaskStarted =>
          processActivityTaskStarted(event)
        case EventType.ActivityTaskCompleted =>
          processActivityTaskCompleted(event)
        case EventType.ActivityTaskFailed =>
          processActivityTaskFailed(event)
        case EventType.ActivityTaskTimedOut =>
          processActivityTaskTimedOut(event)
        case EventType.ActivityTaskCanceled =>
          processActivityTaskCanceled(event)
        case EventType.WorkflowExecutionSignaled =>
          processWorkflowExecutionSignaled(event)
        case EventType.ScheduleActivityTaskFailed =>
          processScheduleActivityTaskFailed(event)
        case EventType.TimerStarted =>
          processTimerStarted(event)
        case EventType.TimerFired =>
          processTimerFired(event)
        //case EventType.Q =>
        //  processQ(event, sections)
        case _ => //println("Unhandled event type: " + event.getEventType)
      }
    }
  } catch {
    case e:Exception =>
      notes += e.getMessage
  }

  protected def ensureSanity() = {
    for((name, section) <- map) {
      for(prereq <- section.prereqs) {
        if(prereq == name) {
          section.status = SectionStatus.IMPOSSIBLE
          val ception = s"Fulfillment is impossible! $name has a self-referential prereq!"
          section.notes += ception
          throw new Exception(ception)
        }
        if(!(map contains prereq)) {
          section.status = SectionStatus.IMPOSSIBLE
          val ception = s"Fulfillment is impossible! Prereq ($prereq) for $name does not exist!"
          section.notes += ception
          throw new Exception(ception)
        }
      }
      for((pname, param) <- section.params) {
        if(pname == name) {
          section.status = SectionStatus.IMPOSSIBLE
          val ception = s"Fulfillment is impossible! $name has a self-referential parameter!"
          section.notes += ception
          throw new Exception(ception)
        }
        param match {
          case sectionReference: SectionReference =>
            val refname = param.asInstanceOf[SectionReference].name
            if(!(map contains refname)) {
              section.status = SectionStatus.IMPOSSIBLE
              val ception = s"Fulfillment is impossible! Param ($pname -> $refname) for $name does not exist!"
              section.notes += ception
              throw new Exception(ception)
            }
          case _ =>
        }
      }
    }
  }

  /**
   * This method builds all of the sections from the initial input to the workflow
   * @param event HistoryEvent
   */
  protected def processWorkflowExecutionStarted(event: HistoryEvent) = {
    val fulfillmentInput:JsValue = Json.parse(event.getWorkflowExecutionStartedEventAttributes.getInput)

    for((jk, jv) <- fulfillmentInput.as[JsObject].fields) {
      map += (jk -> new FulfillmentSection(jk, jv.as[JsObject]))
    }

    ensureSanity()
  }

  protected def processActivityTaskScheduled(event: HistoryEvent) = {
    val activityIdParts = event.getActivityTaskScheduledEventAttributes.getActivityId.split("-")
    val name = activityIdParts(0)

    registry += (event.getEventId -> name)
    map(name).status = SectionStatus.SCHEDULED
    map(name).scheduledCount += 1
  }

  protected def processActivityTaskStarted(event: HistoryEvent) = {
    val name = registry(event.getActivityTaskStartedEventAttributes.getScheduledEventId)
    map(name).status = SectionStatus.STARTED
    map(name).startedCount += 1
  }

  protected def processActivityTaskCompleted(event: HistoryEvent) = {
    val name = registry(event.getActivityTaskCompletedEventAttributes.getScheduledEventId)
    map(name).status = SectionStatus.COMPLETE
    map(name).value = event.getActivityTaskCompletedEventAttributes.getResult
  }

  protected def processActivityTaskFailed(event: HistoryEvent) = {
    val name = registry(event.getActivityTaskFailedEventAttributes.getScheduledEventId)
    map(name).status = SectionStatus.FAILED
    map(name).failedCount += 1
  }

  protected def processActivityTaskTimedOut(event: HistoryEvent) = {
    val name = registry(event.getActivityTaskTimedOutEventAttributes.getScheduledEventId)
    map(name).status = SectionStatus.TIMED_OUT
    map(name).timedoutCount += 1
  }

  protected def processActivityTaskCanceled(event: HistoryEvent) = {
    val name = registry(event.getActivityTaskCanceledEventAttributes.getScheduledEventId)
    map(name).status = SectionStatus.CANCELED
    map(name).canceledCount += 1
  }

  protected def processScheduleActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getScheduleActivityTaskFailedEventAttributes
    val activityIdParts = attribs.getActivityId.split("-")

    val name = activityIdParts(0)

    // FIXME This isn't the typical 'FAILED'. It failed to even get scheduled
    // Not actually sure if this needs to be distinct or not.
    map(name).status = SectionStatus.FAILED
    map(name).failedCount += 1

    notes += s"Failed to schedule activity task because ${attribs.getCause} $activityIdParts"
  }

  protected def processWorkflowExecutionSignaled(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionSignaledEventAttributes
    notes += s"Received signal ${attribs.getSignalName} ${attribs.getInput}"
  }

  protected def processTimerStarted(event: HistoryEvent) = {
    val attribs = event.getTimerStartedEventAttributes

    // Save the timer so we can look it up when it's fired
    timers += (attribs.getTimerId -> attribs.getControl)

    val timerParams:JsObject = Json.parse(attribs.getControl).as[JsObject]
    val sectionName = timerParams.value("section").as[String]

    if(map(sectionName).status == SectionStatus.DEFERRED) {
       map(sectionName).notes += s"ERROR! Section $sectionName was already DEFERRED!!"
    }

    map(sectionName).status = SectionStatus.DEFERRED
  }

  protected def processTimerFired(event: HistoryEvent) = {
    val attribs = event.getTimerFiredEventAttributes

    val timer = timers(attribs.getTimerId)

    val timerParams:JsObject = Json.parse(timer).as[JsObject]
    val sectionName = timerParams.value("section").as[String]
    val status = SectionStatus.withName(timerParams.value("status").as[String])

    map(sectionName).status = status
  }

  override def toString = {
    var out = ""
    for((k, s) <- map) {
      out += s"$k\t\t${s.status.toString}\n"
    }

    out
  }

}

class SectionReference(sectionName: String) {
  val name = sectionName

  override def toString: String = s"section($name)"
}

class FulfillmentCoordinator(swfAdapter: SWFAdapter) {

  val domain = swfAdapter.config.getString("domain")
  val taskListName = swfAdapter.config.getString("tasklist")

  val taskList: TaskList = new TaskList()
    .withName(taskListName)

  val taskReq: PollForDecisionTaskRequest = new PollForDecisionTaskRequest()
    .withDomain(domain)
    .withTaskList(taskList)

  def coordinate() = {

    while(true) {
      print(".")
      val task : DecisionTask = swfAdapter.client.pollForDecisionTask(taskReq)
      if(task.getTaskToken != null) {

        val decisions = new collection.mutable.MutableList[Decision]()
        val sections = new SectionMap(task.getEvents)
        val categorized = new CategorizedSections(sections)
        makeDecisions(decisions, categorized, sections)

        println(sections.notes.mkString("\n"))

        val response:RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest
        response.setTaskToken(task.getTaskToken)
        response.setDecisions(asJavaCollection(decisions))
        swfAdapter.client.respondDecisionTaskCompleted(response)
      }
    }
  }

  protected def gatherParameters(section: FulfillmentSection
                                ,sections: SectionMap) = {

    var params: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]()

    for((name, value) <- section.params) {
      value match {
        case sectionReference: SectionReference =>
          val referencedSection: FulfillmentSection = sections.map(sectionReference.name)
          params += (name -> referencedSection.value)
        case v: String =>
          params += (name -> v)
        case _ =>
          sections.notes += s"Parameter $name doesn't have a recognizable value $value"
      }
    }

    Json.stringify(Json.toJson(params.toMap))

  }

  protected def _createTimerDecision(name:String, delaySeconds:Int, status:String, reason:String) = {

    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.StartTimer)

    var timerParams: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]()
    timerParams += ("section" -> name)
    timerParams += ("status" -> status)
    timerParams += ("reason" -> reason)

    val attribs: StartTimerDecisionAttributes = new StartTimerDecisionAttributes
    attribs.setTimerId(randomUUID().toString)
    attribs.setStartToFireTimeout(delaySeconds.toString)
    attribs.setControl(Json.stringify(Json.toJson(timerParams.toMap)))

    decision.setStartTimerDecisionAttributes(attribs)

    decision
  }

  /**
   *
   * @param decisions Decisions will be added to this list
   * @param categorized The categorized current state of the segments
   * @param sections A Name -> Section mapping helper
   */
  protected def makeDecisions(decisions: collection.mutable.MutableList[Decision]
                             ,categorized: CategorizedSections
                             ,sections: SectionMap): Boolean = {

    var failReasons: mutable.MutableList[String] = mutable.MutableList[String]()

    sections.notes += sections.toString

    if(categorized.workComplete()) {
      // If we're done then let's just bail here
      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.CompleteWorkflowExecution)
      decisions += decision
      sections.notes += "Workflow Complete!!!"
      return false

    }

    if(categorized.impossible.length > 0) {
      var details: String = "Impossible Sections:"
      for(section <- categorized.impossible) {
        details += s"${section.name} ${section.notes}, "
      }
      failReasons += details
    }

    // Loop through the problem sections
    for(section <- categorized.failed) {
      if(section.failedCount < section.failureParams.maxRetries) {
        decisions += _createTimerDecision(section.name, section.failureParams.delaySeconds, SectionStatus.INCOMPLETE.toString,
          s"Section failed and is allowed to retry (${section.failedCount} of ${section.failureParams.maxRetries})")
      } else {
        failReasons += s"Section $section FAILED too many times! (${section.failedCount} of ${section.failureParams.maxRetries})"
      }
    }

    for(section <- categorized.timedout) {
      if(section.timedoutCount < section.timeoutParams.maxRetries) {
        decisions += _createTimerDecision(section.name, section.timeoutParams.delaySeconds, SectionStatus.INCOMPLETE.toString,
          s"Section timed out and is allowed to retry (${section.timedoutCount} of ${section.timeoutParams.maxRetries})")
      } else {
        failReasons += s"Section $section TIMED OUT too many times! (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
      }
    }

    for(section <- categorized.canceled) {
      if(section.canceledCount < section.cancelationParams.maxRetries) {
        decisions += _createTimerDecision(section.name, section.cancelationParams.delaySeconds, SectionStatus.INCOMPLETE.toString,
          s"Section was canceled and is allowed to retry (${section.canceledCount} of ${section.cancelationParams.maxRetries})")
      } else {
        failReasons += s"Section $section was CANCELED too many times! (${section.canceledCount} of ${section.cancelationParams.maxRetries})"
      }
    }

    if(failReasons.length > 0) {

      decisions.clear() // Get rid of any existing decisions.

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.FailWorkflowExecution)

      var details: String = "Failed Sections:"
      details += failReasons.mkString("", "\n\t", "\n")

      // TODO. We should cancel the in-progress sections as BEST as we can
      val attribs: FailWorkflowExecutionDecisionAttributes = new FailWorkflowExecutionDecisionAttributes
      attribs.setReason("There are failed sections!")
      attribs.setDetails(details)

      decision.setFailWorkflowExecutionDecisionAttributes(attribs)

      decisions += decision

      sections.notes += "FAILING!! "+details

      return true
    }

    for(section <- categorized.ready) {
      val params = gatherParameters(section, sections)

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.ScheduleActivityTask)

      val taskList = new TaskList
      taskList.setName(section.action.getName+section.action.getVersion)

      val attribs: ScheduleActivityTaskDecisionAttributes = new ScheduleActivityTaskDecisionAttributes
      attribs.setActivityType(section.action)
      attribs.setInput(params)
      attribs.setTaskList(taskList)
      attribs.setActivityId(section.getActivityId)

      if(section.startToCloseTimeout.nonEmpty) attribs.setStartToCloseTimeout(section.startToCloseTimeout)
      if(section.scheduleToStartTimeout.nonEmpty) attribs.setScheduleToStartTimeout(section.scheduleToStartTimeout)
      if(section.scheduleToCloseTimeout.nonEmpty) attribs.setScheduleToCloseTimeout(section.scheduleToCloseTimeout)
      if(section.heartbeatTimeout.nonEmpty) attribs.setHeartbeatTimeout(section.heartbeatTimeout)

      decision.setScheduleActivityTaskDecisionAttributes(attribs)
      decisions += decision

      sections.notes += s"Scheduling work for ${section.name}"
    }

    if(decisions.length == 0 && !categorized.hasPendingSections) {
      // We aren't making any progress at all! FAIL

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.FailWorkflowExecution)

      var details: String = "Blocked Sections:"
      for(section <- categorized.blocked) {
        details += s"${section.name}, "
      }

      val attribs: FailWorkflowExecutionDecisionAttributes = new FailWorkflowExecutionDecisionAttributes
      attribs.setReason("There blocked sections and nothing is in progress!")
      attribs.setDetails(details)

      decision.setFailWorkflowExecutionDecisionAttributes(attribs)

      decisions += decision

      sections.notes += "FAILING because progress can't be made!"
      for((name, section) <- sections.map) {
        sections.notes += section.toString
      }

    }

    decisions.length == 0
  }
}

object coordinator {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".fulfillment.properties")
    val fc: FulfillmentCoordinator = new FulfillmentCoordinator(new SWFAdapter(config))
    println("Running decider")
    fc.coordinate()
  }
}
