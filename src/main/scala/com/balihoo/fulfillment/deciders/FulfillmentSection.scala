package com.balihoo.fulfillment.deciders

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
  val DISMISSED = Value("DISMISSED") // Section was TERMINAL but a subsequent section may work out
  val COMPLETE = Value("COMPLETE")
  val CONTINGENT = Value("CONTINGENT") // Special case. We won't attempt to process this unless necessary
  val DEFERRED = Value("DEFERRED") // A Timer will activate this later
  val IMPOSSIBLE = Value("IMPOSSIBLE") // Section can never be completed

}

class ActionParams(var maxRetries:Int, var delaySeconds:Int) {
}

class FulfillmentSection(val name: String
                         ,val jsonNode: JsObject) {

  var action: ActivityType = null
  val params = collection.mutable.Map[String, Any]()
  val prereqs = mutable.MutableList[String]()
  val notes = mutable.MutableList[String]()
  var value: String = ""

  var status = SectionStatus.INCOMPLETE

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
              params(jk) = new SectionReference(jArr.as[List[String]])
            case jStr: JsString =>
              params(jk) = jv.as[String]
            case _ =>
              notes += s"Parameter $jv of type ${jv.getClass.toString} for param $jk is not understood!"
          }
        }

      case "prereqs" =>
        val jprereqs = value.as[JsArray]
        prereqs ++= jprereqs.as[List[String]]

      case "status" =>
        status = SectionStatus.withName(value.as[String])

      case _ =>
        notes += s"Section $key unhandled!"
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
    val paramString = "\n"+ (for((k, s) <- params) yield s"\t$k -> $s" ).mkString("\n")
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
 * Build and update FulfillmentSections from the SWF execution history
 * @param history java.util.List[HistoryEvent]
 */
class SectionMap(history: java.util.List[HistoryEvent]) {

  val registry = collection.mutable.Map[java.lang.Long, String]()
  val map = collection.mutable.Map[String, FulfillmentSection]()
  val notes = mutable.MutableList[String]()
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
      for((name, section) <- map) {
        section.resolveReferences(this)
      }

    }
  } catch {
    case e:NoSuchElementException =>
      throw e
    case e:Exception =>
      notes += e.getMessage
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
    (for((k, s) <- map) yield s"$k\t${s.status.toString}").mkString("\n")
  }


}

class SectionReference(val sections:List[String]) {

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

