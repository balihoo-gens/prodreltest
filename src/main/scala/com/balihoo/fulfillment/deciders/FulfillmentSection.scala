package com.balihoo.fulfillment.deciders

import java.util.Date

import com.balihoo.fulfillment.workers.UTCFormatter
import org.joda.time.DateTime

import scala.collection.convert.wrapAsScala._
import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._

import scala.collection.mutable

object SectionStatus extends Enumeration {
  val INCOMPLETE = Value("INCOMPLETE")
  val SCHEDULED = Value("SCHEDULED")
  val STARTED = Value("STARTED")
  val FAILED = Value("FAILED")
  val TIMED_OUT = Value("TIMED OUT")
  val CANCELED = Value("CANCELED")
  val TERMINAL = Value("TERMINAL") // Section has FAILED/CANCELED/TIMED OUT too many times!
  val COMPLETE = Value("COMPLETE")
  val CONTINGENT = Value("CONTINGENT") // Special case. We won't attempt to process this unless necessary
  val DEFERRED = Value("DEFERRED") // A Timer will activate this later
  val IMPOSSIBLE = Value("IMPOSSIBLE") // Section can never be completed

}

class ActionParams(var maxRetries:Int, var delaySeconds:Int) {
}

object TimelineEventType extends Enumeration {
  val NOTE = Value("NOTE")
  val WARNING = Value("WARNING")
  val ERROR = Value("ERROR")
  val SUCCESS = Value("SUCCESS")
}

class TimelineEvent(val eventType:TimelineEventType.Value, val message:String, val when:Date = null) {

  def toJson: JsValue = {
    Json.toJson(Map(
      "eventType" -> Json.toJson(eventType.toString),
      "message" -> Json.toJson(message),
      "when" -> Json.toJson(if(when != null) UTCFormatter.format(when) else "--")
    ))
  }
}

class Timeline {
  val events = mutable.MutableList[TimelineEvent]()

  def error(message:String, when:Date = null) = {
    events += new TimelineEvent(TimelineEventType.ERROR, message, when)
  }

  def warning(message:String, when:Date = null) = {
    events += new TimelineEvent(TimelineEventType.WARNING, message, when)
  }

  def note(message:String, when:Date = null) = {
    events += new TimelineEvent(TimelineEventType.NOTE, message, when)
  }

  def success(message:String, when:Date = null) = {
    events += new TimelineEvent(TimelineEventType.SUCCESS, message, when)
  }
}

class FulfillmentSection(val name: String
                         ,val jsonNode: JsObject
                         ,val creationDate:Date) {

  var action: ActivityType = null
  val params = collection.mutable.Map[String, Any]()
  val prereqs = mutable.MutableList[String]()
  val timeline = new Timeline //mutable.MutableList[TimelineEvent]()
  var value: String = ""

  var status = SectionStatus.INCOMPLETE

  var essential = false

  var scheduledCount: Int = 0
  var startedCount: Int = 0
  var timedoutCount: Int = 0
  var canceledCount: Int = 0
  var failedCount: Int = 0

  val failureParams = new ActionParams(0, 0)
  val timeoutParams = new ActionParams(0, 0)
  val cancelationParams = new ActionParams(0, 0)

  var startToCloseTimeout = ""
  var scheduleToStartTimeout = ""
  var scheduleToCloseTimeout = ""
  var heartbeatTimeout = ""

  var waitUntil: DateTime = null

  for((key, value) <- jsonNode.fields) {
    key match {
      case "action" =>
        val jaction = value.as[JsObject]
        action = new ActivityType
        action.setName(jaction.value("name").as[String])
        action.setVersion(jaction.value("version").as[String])
        handleActionParams(jaction)

      case "params" =>
        val jparams = value.as[JsObject]
        for((jk, jv) <- jparams.fields) {
          jv match {
            case jArr: JsArray =>
              params(jk) = new SectionReferences(jArr.as[List[String]])
            case jStr: JsString =>
              params(jk) = jv.as[String]
            case _ =>
              timeline.error(s"Parameter '$jk' is of type '${jv.getClass.toString}'. This is not a valid type.", creationDate)
          }
        }

      case "prereqs" =>
        val jprereqs = value.as[JsArray]
        prereqs ++= jprereqs.as[List[String]]

      case "status" =>
        status = SectionStatus.withName(value.as[String])

      case "essential" =>
        essential = value.as[Boolean]

      case "waitUntil" =>
        waitUntil = new DateTime(value.as[String])

      case _ =>
        timeline.warning(s"Section input '$key' unhandled!", creationDate)
    }
  }

  def handleActionParams(jaction:JsObject) = {
    for((akey, avalue) <- jaction.fields) {
      akey match {
        case "failure" =>
          val failure = avalue.as[JsObject]
          failureParams.maxRetries =  failure.value("max").as[String].toInt
          failureParams.delaySeconds =  failure.value("delay").as[String].toInt
        case "timeout" =>
          val timeout = avalue.as[JsObject]
          timeoutParams.maxRetries =  timeout.value("max").as[String].toInt
          timeoutParams.delaySeconds =  timeout.value("delay").as[String].toInt
        case "cancelation" =>
          val cancelation = avalue.as[JsObject]
          cancelationParams.maxRetries =  cancelation.value("max").as[String].toInt
          cancelationParams.delaySeconds =  cancelation.value("delay").as[String].toInt
        case "startToCloseTimeout" =>
          startToCloseTimeout = avalue.as[String]
        case "scheduleToCloseTimeout" =>
          startToCloseTimeout = avalue.as[String]
        case "scheduleToStartTimeout" =>
          startToCloseTimeout = avalue.as[String]
        case "heartbeatTimeout" =>
          startToCloseTimeout = avalue.as[String]
        case _ =>
      }
    }
  }

  def setStarted(when:Date) = {
    startedCount += 1
    status = SectionStatus.STARTED
    timeline.note("Started", when)
  }

  def setScheduled(when:Date) = {
    scheduledCount += 1
    status = SectionStatus.SCHEDULED
    timeline.note("Scheduled", when)
  }

  def setCompleted(result:String, when:Date) = {
    status = SectionStatus.COMPLETE
    timeline.note("Completed", when)
    value = result
  }

  def setFailed(reason:String, details:String, when:Date) = {
    failedCount += 1
    timeline.warning(s"Failed because:$reason $details", when)
    status = if(failedCount > failureParams.maxRetries) SectionStatus.TERMINAL else SectionStatus.FAILED
  }

  def setCanceled(details:String, when:Date) = {
    canceledCount += 1
    timeline.warning(s"Canceled because: $details", when)
    status = if(canceledCount > cancelationParams.maxRetries) SectionStatus.TERMINAL else SectionStatus.CANCELED
  }

  def setTimedOut(when:Date) = {
    timedoutCount += 1
    timeline.warning("Timed out!", when)
    status = if(timedoutCount > timeoutParams.maxRetries) SectionStatus.TERMINAL else SectionStatus.TIMED_OUT
  }

  def resolveReferences(map:SectionMap):Boolean = {

    if(status == SectionStatus.INCOMPLETE) {
      for((pname, param) <- params) {
        param match {
          case sectionReferences: SectionReferences =>
            sectionReferences.processReferences(map)
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
    Json.stringify(toJson)
  }

  def toJson: JsValue = {
    val jparams = collection.mutable.Map[String, JsValue]()
    for((pname, param) <- params) {
      param match {
        case sectionReferences: SectionReferences =>
          jparams(pname) = sectionReferences.toJson
        case s:String =>
          jparams(pname) = Json.toJson(s)
        case _ =>
      }
    }

    val jtimeline = Json.toJson(for(entry <- timeline.events) yield entry.toJson)

    Json.toJson(Map(
      "status" -> Json.toJson(status.toString),
      "timeline" -> Json.toJson(jtimeline),
      "value" -> Json.toJson(value),
      "input" -> Json.toJson(jsonNode),
      "params" -> Json.toJson(jparams.toMap),
      "essential" -> Json.toJson(essential),
      "scheduledCount" -> Json.toJson(scheduledCount),
      "startedCount" -> Json.toJson(startedCount),
      "timedoutCount" -> Json.toJson(timedoutCount),
      "canceledCount" -> Json.toJson(canceledCount),
      "failedCount" -> Json.toJson(failedCount)
    ))
  }
}

/**
 * Build and update FulfillmentSections from the SWF execution history
 * @param history java.util.List[HistoryEvent]
 */
class SectionMap(history: java.util.List[HistoryEvent]) {

  val registry = collection.mutable.Map[java.lang.Long, String]()
  val nameToSection = collection.mutable.Map[String, FulfillmentSection]()
  val timeline = new Timeline //mutable.MutableList[TimelineEvent]()
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

      // We look through the section references to see if anything needs to be promoted from CONTINGENT -> INCOMPLETE.
      // Sections get promoted when they're in a SectionReference list and the prior section is TERMINAL
      // We also check to see if sections
      for((name, section) <- nameToSection) {
        section.resolveReferences(this)
      }

    }
  } catch {
    case e:NoSuchElementException =>
      throw e
    case e:Exception =>
      timeline.error(e.getMessage, new Date())
  }

  /**
   * This checks for situations that would make fulfillment impossible
   */
  protected def ensureSanity(when:Date) = {
    var essentialCount = 0
    for((name, section) <- nameToSection) {
      if(section.essential) { essentialCount += 1 }
      for(prereq <- section.prereqs) {
        if(prereq == name) {
          section.status = SectionStatus.IMPOSSIBLE
          val ception = s"Fulfillment is impossible! $name has a self-referential prereq!"
          section.timeline.error(ception, when)
          throw new Exception(ception)
        }
        if(!hasSection(prereq)) {
          section.status = SectionStatus.IMPOSSIBLE
          val ception = s"Fulfillment is impossible! Prereq ($prereq) for $name does not exist!"
          section.timeline.error(ception, when)
          throw new Exception(ception)
        }
      }
      for((pname, param) <- section.params) {
        if(pname == name) {
          section.status = SectionStatus.IMPOSSIBLE
          val ception = s"Fulfillment is impossible! $name has a self-referential parameter!"
          section.timeline.error(ception, when)
          throw new Exception(ception)
        }
        param match {
          case sectionReferences: SectionReferences =>
            for(sectionRef <- sectionReferences.sections) {
              if(!hasSection(sectionRef.name)) {
                section.status = SectionStatus.IMPOSSIBLE
                val ception = s"Fulfillment is impossible! Param ($pname -> ${sectionRef.name}) for $name does not exist!"
                section.timeline.error(ception, when)
                throw new Exception(ception)
              }
            }
          case _ =>
        }
      }
    }

    if(0 == essentialCount) {
      timeline.warning("No essential sections!", when)
    }
  }

  protected def getSectionByName(name:String): FulfillmentSection = {
    nameToSection(name)
  }

  protected def getSectionById(id:Long): FulfillmentSection = {
    nameToSection(registry(id))
  }

  protected def hasSection(name:String): Boolean = {
    nameToSection contains name
  }

  /**
   * This method builds all of the sections from the initial input to the workflow
   * @param event HistoryEvent
   */
  protected def processWorkflowExecutionStarted(event: HistoryEvent) = {
    val fulfillmentInput = Json.parse(event.getWorkflowExecutionStartedEventAttributes.getInput).as[JsObject]

    for((jk, jv) <- fulfillmentInput.fields) {
      nameToSection += (jk -> new FulfillmentSection(jk, jv.as[JsObject], event.getEventTimestamp))
    }

    ensureSanity(event.getEventTimestamp)
  }

  protected def processActivityTaskScheduled(event: HistoryEvent) = {
    val activityIdParts = event.getActivityTaskScheduledEventAttributes.getActivityId.split(Constants.delimiter)
    val name = activityIdParts(0)
    registry += (event.getEventId -> name)
    getSectionByName(name).setScheduled(event.getEventTimestamp)
  }

  protected def processActivityTaskStarted(event: HistoryEvent) = {
    val attribs = event.getActivityTaskStartedEventAttributes
    getSectionById(attribs.getScheduledEventId).setStarted(event.getEventTimestamp)
  }

  protected def processActivityTaskCompleted(event: HistoryEvent) = {
    val attribs = event.getActivityTaskCompletedEventAttributes
    getSectionById(attribs.getScheduledEventId).setCompleted(attribs.getResult, event.getEventTimestamp)
  }

  protected def processActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getActivityTaskFailedEventAttributes
    getSectionById(attribs.getScheduledEventId).setFailed(attribs.getReason, attribs.getDetails, event.getEventTimestamp)
  }

  protected def processActivityTaskTimedOut(event: HistoryEvent) = {
    val attribs = event.getActivityTaskTimedOutEventAttributes
    getSectionById(attribs.getScheduledEventId).setTimedOut(event.getEventTimestamp)
  }

  protected def processActivityTaskCanceled(event: HistoryEvent) = {
    val attribs = event.getActivityTaskCanceledEventAttributes
    getSectionById(attribs.getScheduledEventId).setCanceled(attribs.getDetails, event.getEventTimestamp)
  }

  protected def processScheduleActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getScheduleActivityTaskFailedEventAttributes
    val activityIdParts = attribs.getActivityId.split(Constants.delimiter)

    val name = activityIdParts(0)

    // FIXME This isn't the typical 'FAILED'. It failed to even get scheduled
    // Not actually sure if this needs to be distinct or not.
    nameToSection(name).setFailed("Failed to Schedule task!", attribs.getCause, event.getEventTimestamp)

  }

  protected def processWorkflowExecutionSignaled(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionSignaledEventAttributes
    timeline.note(s"Received signal ${attribs.getSignalName} ${attribs.getInput}", event.getEventTimestamp)
  }

  protected def processTimerStarted(event: HistoryEvent) = {
    val attribs = event.getTimerStartedEventAttributes

    // Save the timer so we can look it up when it's fired
    timers += (attribs.getTimerId -> attribs.getControl)

    val timerParams:JsObject = Json.parse(attribs.getControl).as[JsObject]
    val sectionName = timerParams.value("section").as[String]

    val section = getSectionByName(sectionName)

    if(section.status == SectionStatus.DEFERRED) {
      section.timeline.error(s"$sectionName is already DEFERRED!!", event.getEventTimestamp)
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
    (for((k, s) <- nameToSection) yield s"$k\t${s.status.toString}").mkString("\n")
  }
}

class SectionReference(val name:String) {
  var dismissed:Boolean = false
  var section:FulfillmentSection = null

  def toJson:JsValue = {
    Json.toJson(Map(
      "name" -> Json.toJson(name),
      "dismissed" -> Json.toJson(dismissed)
    ))
  }
}

class SectionReferences(sectionNames:List[String]) {

  val sections = for(name <- sectionNames) yield new SectionReference(name)

  def hydrate(map:SectionMap) = {
    for(sectionRef <- sections) {
      sectionRef.section = map.nameToSection(sectionRef.name)
    }
  }

  def processReferences(map:SectionMap) = {
    hydrate(map)

    var priorSectionRef:SectionReference = null

    for(sectionRef <- sections) {
      priorSectionRef match {
        case sr: SectionReference =>
          sr.section.status match {
            case SectionStatus.TERMINAL =>
              if(sectionRef.section.status == SectionStatus.CONTINGENT) {

                // The prior section didn't complete successfully.. let's
                // let the next section have a try
                sectionRef.section.status = SectionStatus.INCOMPLETE
                sectionRef.section.resolveReferences(map) // <-- recurse
              }
            case _ => // We don't care about other status until a TERMINAL case is hit
          }
          priorSectionRef.dismissed = true
        case _ =>
          // This is the first referenced section..
      }
      priorSectionRef = sectionRef
    }
  }

  def resolved(map:SectionMap):Boolean = {
    hydrate(map)

    for(sectionRef <- sections) {
      sectionRef.section.status match {
        case SectionStatus.COMPLETE =>
          return true
        case _ =>
      }
    }
    false
  }

  def getValue(map:SectionMap):String = {
    hydrate(map)

    for(sectionRef <- sections) {
      if(sectionRef.section.status == SectionStatus.COMPLETE) {
        return sectionRef.section.value
      }
    }

    val gripe = "Tried to get value from referenced sections and no value was available! "+toString()
    map.timeline.error(gripe)

    throw new Exception(gripe)
  }

  override def toString: String = s"sections($sectionNames)"

  def toJson:JsValue = {
    Json.toJson(
      for(section <- sections) yield section.toJson
    )
  }
}

