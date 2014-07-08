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

object Constants {
  final val delimiter = "##"
}

object SectionStatus extends Enumeration {
  val INCOMPLETE = Value("INCOMPLETE")
  val SCHEDULED = Value("SCHEDULED")
  val STARTED = Value("STARTED")
  val FAILED = Value("FAILED")
  val TIMED_OUT = Value("TIMED OUT")
  val CANCELED = Value("CANCELED")
  val TERMINAL = Value("TERMINAL") // Section has FAILED/CANCELED/TIMED OUT too many times!
  val DISMISSED = Value("DISMISSED") // Section was TERMINAL but a subsequent section may work out
  val COMPLETE = Value("COMPLETE")
  val CONTINGENT = Value("CONTINGENT") // Special case. We won't attempt to process this unless necessary
  val DEFERRED = Value("DEFERRED") // A Timer will activate this later
  val IMPOSSIBLE = Value("IMPOSSIBLE") // Section can never be completed

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
            case jArr: JsArray =>
              params += (jk -> new SectionReference(jArr))
            case jStr: JsString =>
              params += (jk -> jv.as[String])
            case _ =>
              notes += s"Parameter $jv of type ${jv.getClass.toString} for param $jk is not understood!"
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

  def setStarted() = {
    startedCount += 1
    status = SectionStatus.STARTED
  }

  def setScheduled() = {
    scheduledCount += 1
    status = SectionStatus.SCHEDULED
  }

  def setCompleted(result:String) = {
    status = SectionStatus.COMPLETE
    value = result
  }

  def setFailed(reason:String, details:String) = {
    failedCount += 1
    notes += "Failed because: "+reason
    notes += details
    status = if(failedCount > failureParams.maxRetries) SectionStatus.TERMINAL else SectionStatus.FAILED
  }

  def setCanceled(details:String) = {
    canceledCount += 1
    notes += "Canceled because: "+details
    status = if(canceledCount > cancelationParams.maxRetries) SectionStatus.TERMINAL else SectionStatus.CANCELED
  }

  def setTimedOut() = {
    timedoutCount += 1
    notes += "Timed out!"
    status = if(timedoutCount > timeoutParams.maxRetries) SectionStatus.TERMINAL else SectionStatus.TIMED_OUT
  }

  def resolveReferences(map:SectionMap):Boolean = {

    if(status == SectionStatus.INCOMPLETE) {
      for((pname, param) <- params) {
        param match {
          case sectionReference: SectionReference =>
            sectionReference.processReferences(map)
          case _ =>
        }
      }
    }
    true
  }

  def getActivityId = {
    val timestamp: Long = System.currentTimeMillis()
    s"$name${Constants.delimiter}${action.getName}${Constants.delimiter}"+timestamp
  }

  override def toString = {
    var paramString = "\n"
    for((k, s) <- params) {
      paramString += s"\t\t$k -> $s\n"
    }
    s"""$name $status
      |  Action: $action
      |  Params: $paramString
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
  var complete = mutable.MutableList[FulfillmentSection]()
  var inprogress = mutable.MutableList[FulfillmentSection]()
  var timedout = mutable.MutableList[FulfillmentSection]()
  var deferred = mutable.MutableList[FulfillmentSection]()
  var blocked = mutable.MutableList[FulfillmentSection]()
  var failed = mutable.MutableList[FulfillmentSection]()
  var canceled = mutable.MutableList[FulfillmentSection]()
  var contingent = mutable.MutableList[FulfillmentSection]()
  var dismissed = mutable.MutableList[FulfillmentSection]()
  var terminal = mutable.MutableList[FulfillmentSection]()
  var ready = mutable.MutableList[FulfillmentSection]()
  var impossible = mutable.MutableList[FulfillmentSection]()

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
      case SectionStatus.CONTINGENT =>
        contingent += section
      case SectionStatus.TIMED_OUT =>
        timedout += section
      case SectionStatus.DEFERRED =>
        deferred += section
      case SectionStatus.TERMINAL =>
        terminal += section
      case SectionStatus.DISMISSED =>
        dismissed += section
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
          paramsReady &= sectionReference.resolved(sections)
        case _ =>
          // non-reference params are automatically ready..
      }
    }

    var prereqsReady: Boolean = true
    for(prereq: String <- section.prereqs) {
      val referencedSection: FulfillmentSection = sections.map(prereq)
      referencedSection.status match {
        case SectionStatus.COMPLETE =>
          // println("Section is complete")
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
    sections.map.size == (complete.length + contingent.length + dismissed.length)
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
  var notes = mutable.MutableList[String]()
  val timers = collection.mutable.Map[String, String]()

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
    case e:NoSuchElementException =>
      throw e
    case e:Exception =>
      notes += e.getMessage
  }

  // Now that all of the pending events have been processed, we look through the
  // section references to see if anything needs to be promoted from CONTINGENT -> INCOMPLETE.
  // Sections get promoted when they're in a SectionReference list and the prior section is TERMINAL
  for((name, section) <- map) {
    section.resolveReferences(this)
  }

  /**
   * This checks for situations that would make fulfillment impossible
   */
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
            for(sectionName <- sectionReference.sections) {
              if(!(map contains sectionName)) {
                section.status = SectionStatus.IMPOSSIBLE
                val ception = s"Fulfillment is impossible! Param ($pname -> $sectionName) for $name does not exist!"
                section.notes += ception
                throw new Exception(ception)
              }
            }
          case _ =>
        }
      }
    }
  }

  protected def getSectionByName(name:String): FulfillmentSection = {
    map(name)
  }

  protected def getSectionById(id:Long): FulfillmentSection = {
    map(registry(id))
  }

  /**
   * This method builds all of the sections from the initial input to the workflow
   * @param event HistoryEvent
   */
  protected def processWorkflowExecutionStarted(event: HistoryEvent) = {
    val fulfillmentInput = Json.parse(event.getWorkflowExecutionStartedEventAttributes.getInput).as[JsObject]

    for((jk, jv) <- fulfillmentInput.fields) {
      map += (jk -> new FulfillmentSection(jk, jv.as[JsObject]))
    }

    ensureSanity()
  }

  protected def processActivityTaskScheduled(event: HistoryEvent) = {
    val activityIdParts = event.getActivityTaskScheduledEventAttributes.getActivityId.split(Constants.delimiter)
    val name = activityIdParts(0)
    registry += (event.getEventId -> name)
    getSectionByName(name).setScheduled()
  }

  protected def processActivityTaskStarted(event: HistoryEvent) = {
    val attribs = event.getActivityTaskStartedEventAttributes
    getSectionById(attribs.getScheduledEventId).setStarted()
  }

  protected def processActivityTaskCompleted(event: HistoryEvent) = {
    val attribs = event.getActivityTaskCompletedEventAttributes
    getSectionById(attribs.getScheduledEventId).setCompleted(attribs.getResult)
  }

  protected def processActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getActivityTaskFailedEventAttributes
    getSectionById(attribs.getScheduledEventId).setFailed(attribs.getReason, attribs.getDetails)
  }

  protected def processActivityTaskTimedOut(event: HistoryEvent) = {
    val attribs = event.getActivityTaskTimedOutEventAttributes
    getSectionById(attribs.getScheduledEventId).setTimedOut()
  }

  protected def processActivityTaskCanceled(event: HistoryEvent) = {
    val attribs = event.getActivityTaskCanceledEventAttributes
    getSectionById(attribs.getScheduledEventId).setCanceled(attribs.getDetails)
  }

  protected def processScheduleActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getScheduleActivityTaskFailedEventAttributes
    val activityIdParts = attribs.getActivityId.split(Constants.delimiter)

    val name = activityIdParts(0)

    // FIXME This isn't the typical 'FAILED'. It failed to even get scheduled
    // Not actually sure if this needs to be distinct or not.
    map(name).setFailed("Failed to Schedule task!", attribs.getCause)

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

    val section = getSectionByName(sectionName)

    if(section.status == SectionStatus.DEFERRED) {
       section.notes += s"ERROR! Section $sectionName was already DEFERRED!!"
    }

    section.status = SectionStatus.DEFERRED
  }

  protected def processTimerFired(event: HistoryEvent) = {
    val attribs = event.getTimerFiredEventAttributes

    val timer = timers(attribs.getTimerId)

    val timerParams:JsObject = Json.parse(timer).as[JsObject]
    val sectionName = timerParams.value("section").as[String]
    val status = SectionStatus.withName(timerParams.value("status").as[String])

    getSectionByName(sectionName).status = status
  }

  override def toString = {
    var out = ""
    for((k, s) <- map) {
      out += s"$k\t\t${s.status.toString}\n"
    }
    out
  }

}

class SectionReference(referencedSections:JsArray) {
  var sections = mutable.MutableList[String]()
  for(sectionName <- referencedSections.as[List[String]]) {
    sections += sectionName
  }

  def processReferences(map:SectionMap) = {
    var priorSection:FulfillmentSection = null
    for(sectionName <- sections) {
      val referencedSection = map.map(sectionName)
      if((priorSection == null || priorSection.status == SectionStatus.TERMINAL)
        && referencedSection.status == SectionStatus.CONTINGENT) {
        // The prior section didn't complete successfully.. let's
        // let the next section have a try
        referencedSection.status = SectionStatus.INCOMPLETE
        referencedSection.resolveReferences(map) // <-- recurse
        if(priorSection != null) {
          priorSection.status = SectionStatus.DISMISSED
        }
      }
      priorSection = referencedSection
    }
  }

  def resolved(map:SectionMap):Boolean = {
    for(sectionName <- sections) {
      val referencedSection = map.map(sectionName)
      referencedSection.status match {
        case SectionStatus.COMPLETE =>
          return true
        case _ =>
      }
    }
    false
  }

  def getValue(map:SectionMap):String = {
    var sectionsSummary = "Sections("
    for(sectionName <- sections) {
      val referencedSection = map.map(sectionName)
      sectionsSummary += s"sectionName:${referencedSection.status} "
      if(referencedSection.status == SectionStatus.COMPLETE) {
        return referencedSection.value
      }
    }
    sectionsSummary += ")"
    throw new Exception("Tried to get value from referenced sections and no value was available! "+sectionsSummary)
  }

  override def toString: String = s"section($sections)"
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
      try {
        val task: DecisionTask = swfAdapter.client.pollForDecisionTask(taskReq)

        if(task.getTaskToken != null) {

          val sections = new SectionMap(task.getEvents)
          val categorized = new CategorizedSections(sections)
          val decisions = makeDecisions(categorized, sections)

          println(sections.notes.mkString("\n"))

          val response: RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest
          response.setTaskToken(task.getTaskToken)
          response.setDecisions(asJavaCollection(decisions))
          swfAdapter.client.respondDecisionTaskCompleted(response)
        }
      } catch {
        case e:Exception =>
          println("\n"+e.getMessage)
        case t:Throwable =>
          println("\n"+t.getMessage)
      }
    }
  }

  protected def gatherParameters(section: FulfillmentSection
                                ,sections: SectionMap) = {

    var params = collection.mutable.Map[String, String]()

    for((name, value) <- section.params) {
      value match {
        case sectionReference: SectionReference =>
          params += (name -> sectionReference.getValue(sections))
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

    var timerParams = collection.mutable.Map[String, String]()
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
   * @param categorized The categorized current state of the segments
   * @param sections A Name -> Section mapping helper
   */
  protected def makeDecisions(categorized: CategorizedSections
                             ,sections: SectionMap): collection.mutable.MutableList[Decision] = {

    val decisions = new collection.mutable.MutableList[Decision]()

    var failReasons = mutable.MutableList[String]()

    sections.notes += sections.toString

    if(categorized.workComplete()) {
      // If we're done then let's just bail here
      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.CompleteWorkflowExecution)
      decisions += decision
      sections.notes += "Workflow Complete!!!"
      return decisions

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

      return decisions
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

      sections.notes += "Scheduling work for: "+section.toString
    }

    if(decisions.length == 0 && !categorized.hasPendingSections) {
      // We aren't making any progress at all! FAIL

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.FailWorkflowExecution)
      val attribs = new FailWorkflowExecutionDecisionAttributes

      sections.notes += "Workflow FAILED:"
      if(categorized.blocked.length > 0) {
        var details: String = "Blocked Sections:"
        for(section <- categorized.blocked) {
          details += s"${section.name}, "
        }

        attribs.setReason("There are blocked sections and nothing is in progress!")
        attribs.setDetails(details)
        sections.notes += details
      } else {

        var details: String = "Sections:"
        for((name, section) <- sections.map) {
          details += section.toString
        }

        attribs.setReason("FAILING because progress can't be made!")
        attribs.setDetails(details)
      }

      decision.setFailWorkflowExecutionDecisionAttributes(attribs)

      decisions += decision

    }

    decisions
  }
}

object coordinator {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val fc: FulfillmentCoordinator = new FulfillmentCoordinator(new SWFAdapter(config))
    println("Running decider")
    fc.coordinate()
  }
}
