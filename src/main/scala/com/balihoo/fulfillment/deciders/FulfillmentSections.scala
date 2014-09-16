package com.balihoo.fulfillment.deciders

import org.joda.time.DateTime

import scala.collection.convert.wrapAsScala._
import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._

/**
 * Build and update FulfillmentSections from the SWF execution history
 * @param history java.util.List[HistoryEvent]
 */
class FulfillmentSections(history: java.util.List[HistoryEvent]) {

  val eventHandlers = collection.mutable.Map[EventType, (HistoryEvent) => (Any)]()
  val registry = collection.mutable.Map[java.lang.Long, String]()
  val nameToSection = collection.mutable.Map[String, FulfillmentSection]()
  val timeline = new Timeline
  val timers = collection.mutable.Map[String, String]()
  var resolution = "IN PROGRESS"

  _addEventHandler(EventType.WorkflowExecutionStarted, processWorkflowExecutionStarted)
  _addEventHandler(EventType.ActivityTaskScheduled, processActivityTaskScheduled)
  _addEventHandler(EventType.ActivityTaskStarted, processActivityTaskStarted)
  _addEventHandler(EventType.ActivityTaskCompleted, processActivityTaskCompleted)
  _addEventHandler(EventType.ActivityTaskFailed, processActivityTaskFailed)
  _addEventHandler(EventType.ActivityTaskTimedOut, processActivityTaskTimedOut)
  _addEventHandler(EventType.ActivityTaskCanceled, processActivityTaskCanceled)
  _addEventHandler(EventType.WorkflowExecutionSignaled, processWorkflowExecutionSignaled)
  _addEventHandler(EventType.ScheduleActivityTaskFailed, processScheduleActivityTaskFailed)
  _addEventHandler(EventType.TimerStarted, processTimerStarted)
  _addEventHandler(EventType.TimerFired, processTimerFired)
  _addEventHandler(EventType.MarkerRecorded, processMarkerRecorded)
  _addEventHandler(EventType.RecordMarkerFailed, processRecordMarkerFailed)
  _addEventHandler(EventType.WorkflowExecutionCanceled, processCancel)
  _addEventHandler(EventType.WorkflowExecutionTimedOut, processTimedOut)
  _addEventHandler(EventType.WorkflowExecutionTerminated, processTerminated)
  _addEventHandler(EventType.WorkflowExecutionFailed, processFailed)
  _addEventHandler(EventType.WorkflowExecutionCompleted, processCompleted)
  _addEventHandler(EventType.DecisionTaskScheduled, processIgnoredEventType)
  _addEventHandler(EventType.DecisionTaskStarted, processIgnoredEventType)
  _addEventHandler(EventType.DecisionTaskCompleted, processIgnoredEventType)

  try {
    for(event: HistoryEvent <- collectionAsScalaIterable(history)) {
      processEvent(event)
    }
  } catch {
    case e:Exception =>
      timeline.error(e.getMessage, Some(DateTime.now))
      resolution = "FAILED"
  }

  // Now that all of the HistoryEvents have been processed our sections have been created and are up to date.
  val categorized = new CategorizedSections(this)

  protected def _addEventHandler(eventType:EventType, handler:(HistoryEvent)=>(Any)) = {
    eventHandlers(eventType) = handler
  }

  /**
   * This checks for situations that would make fulfillment impossible
   */
  protected def ensureSanity(when:DateTime) = {
    var essentialCount = 0
    for((name, section) <- nameToSection) {
      if(section.essential) { essentialCount += 1 }
      for(prereq <- section.prereqs) {
        if(prereq == name) {
          val ception = s"Fulfillment is impossible! $name has a self-referential prereq!"
          section.setImpossible(ception, when)
          throw new Exception(ception)
        }
        if(!hasSection(prereq)) {
          val ception = s"Fulfillment is impossible! Prereq ($prereq) for $name does not exist!"
          section.setImpossible(ception, when)
          throw new Exception(ception)
        }
      }
      for((pname, param) <- section.params) {
        if(pname == name) {
          val ception = s"Fulfillment is impossible! $name has a self-referential parameter!"
          section.setImpossible(ception, when)
          throw new Exception(ception)
        }
        param match {
          case sectionReferences: SectionReferences =>
            for(sectionRef <- sectionReferences.sections) {
              if(!hasSection(sectionRef.name)) {
                val ception = s"Fulfillment is impossible! Param ($pname -> ${sectionRef.name}) for $name does not exist!"
                section.setImpossible(ception, when)
                throw new Exception(ception)
              }
            }
          case _ =>
        }
      }
    }

    if(0 == essentialCount) {
      timeline.warning("No essential sections!", Some(when))
    }
  }

  def size():Int = {
    nameToSection.size
  }

  def getSectionByName(name:String): FulfillmentSection = {
    try {
      nameToSection(name)
    } catch {
      case nsee:NoSuchElementException =>
        throw new Exception(s"There is no section '$name'", nsee)
      case e:Exception =>
        throw new Exception(s"Error while looking up section '$name'", e)
    }
  }

  private def getSectionById(id:Long): FulfillmentSection = {
    nameToSection(registry(id))
  }

  def hasSection(name:String): Boolean = {
    nameToSection isDefinedAt name
  }

  protected def processEvent(event:HistoryEvent) = {
    try {
      eventHandlers.getOrElse(EventType.fromValue(event.getEventType), processUnhandledEventType _)(event)
    } catch {
      case e:Exception =>
        timeline.error(s"Problem processing ${event.getEventType}: "+e.getMessage, Some(new DateTime(event.getEventTimestamp)))
    }

    // We look through the section references to see if anything needs to be promoted from CONTINGENT -> READY.
    // Sections get promoted when they're in a SectionReference list and the prior section is TERMINAL
    for((name, section) <- nameToSection) {
      section.resolveReferences(this)
    }
  }

  protected def processUnhandledEventType(event:HistoryEvent) = {
    timeline.warning(s"Event type:${event.getEventType} unhandled.", Some(new DateTime(event.getEventTimestamp)))
  }

  protected def processIgnoredEventType(event:HistoryEvent) = {
  }
  /**
   * This method builds all of the sections from the initial input to the workflow
   * @param event HistoryEvent
   */
  protected def processWorkflowExecutionStarted(event: HistoryEvent) = {
    val fulfillmentInput = Json.parse(event.getWorkflowExecutionStartedEventAttributes.getInput).as[JsObject]

    for((jk, jv) <- fulfillmentInput.fields) {
      nameToSection += (jk -> new FulfillmentSection(jk, jv.as[JsObject], new DateTime(event.getEventTimestamp)))
    }

    ensureSanity(new DateTime(event.getEventTimestamp))
  }

  protected def processActivityTaskScheduled(event: HistoryEvent) = {
    val activityIdParts = event.getActivityTaskScheduledEventAttributes.getActivityId.split(Constants.delimiter)
    val name = activityIdParts(0)
    registry += (event.getEventId -> name)
    getSectionByName(name).setScheduled(new DateTime(event.getEventTimestamp))
  }

  protected def processActivityTaskStarted(event: HistoryEvent) = {
    val attribs = event.getActivityTaskStartedEventAttributes
    getSectionById(attribs.getScheduledEventId).setStarted(new DateTime(event.getEventTimestamp))
  }

  protected def processActivityTaskCompleted(event: HistoryEvent) = {
    val attribs = event.getActivityTaskCompletedEventAttributes
    getSectionById(attribs.getScheduledEventId).setCompleted(attribs.getResult, new DateTime(event.getEventTimestamp))
  }

  protected def processActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getActivityTaskFailedEventAttributes
    getSectionById(attribs.getScheduledEventId).setFailed(attribs.getReason, attribs.getDetails, new DateTime(event.getEventTimestamp))
  }

  protected def processActivityTaskTimedOut(event: HistoryEvent) = {
    val attribs = event.getActivityTaskTimedOutEventAttributes
    getSectionById(attribs.getScheduledEventId).setTimedOut(attribs.getTimeoutType, attribs.getDetails, new DateTime(event.getEventTimestamp))
  }

  protected def processActivityTaskCanceled(event: HistoryEvent) = {
    val attribs = event.getActivityTaskCanceledEventAttributes
    getSectionById(attribs.getScheduledEventId).setCanceled(attribs.getDetails, new DateTime(event.getEventTimestamp))
  }

  protected def processScheduleActivityTaskFailed(event: HistoryEvent) = {
    val attribs = event.getScheduleActivityTaskFailedEventAttributes
    val activityIdParts = attribs.getActivityId.split(Constants.delimiter)

    val name = activityIdParts(0)

    // FIXME This isn't the typical 'FAILED'. It failed to even get scheduled
    // Not actually sure if this needs to be distinct or not.
    getSectionByName(name).setFailed("Failed to Schedule task!", attribs.getCause, new DateTime(event.getEventTimestamp))

  }

  protected def processWorkflowExecutionSignaled(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionSignaledEventAttributes

    attribs.getSignalName match {
      case "sectionUpdates" =>
        val updates = Json.parse(attribs.getInput).as[JsObject]
        for((sectionName, iupdate:JsValue) <- updates.fields) {
          val update = iupdate.as[JsObject]
          val section = getSectionByName(sectionName)
          for((updateType, body:JsValue) <- update.fields) {
            updateType match {
              case "params" =>
                val pupdate = Json.stringify(body)
                section.timeline.note(s"Updating params: $pupdate", Some(new DateTime(event.getEventTimestamp)))
                section.jsonInitParams(body.as[JsObject])
              case "status" =>
                val supdate = body.as[String]
                section.timeline.note(s"Updating status: ${section.status} -> $supdate", Some(new DateTime(event.getEventTimestamp)))
                try {
                  section.setStatus(supdate, "sectionUpdate(status)", new DateTime(event.getEventTimestamp))
                } catch {
                  case nsee:NoSuchElementException =>
                    section.timeline.error(s"Status $supdate is INVALID!", None)
                }
              case "essential" =>
                val eupdate = body.as[Boolean]
                section.timeline.note(s"Updating essential: $eupdate", Some(new DateTime(event.getEventTimestamp)))
                section.essential = eupdate
              case "action" =>
                val aupdate = Json.stringify(body)
                section.timeline.note(s"Updating action: $aupdate", Some(new DateTime(event.getEventTimestamp)))
                section.jsonInitAction(body.as[JsObject])
              case "prereqs" =>
                val pupdate = Json.stringify(body)
                section.timeline.note(s"Updating prereqs: $pupdate", Some(new DateTime(event.getEventTimestamp)))
                section.jsonInitPrereqs(body.as[JsArray])
              case _ =>
            }
          }
        }
      case _ =>
        timeline.warning(s"Unhandled signal ${attribs.getSignalName} ${attribs.getInput}", Some(new DateTime(event.getEventTimestamp)))
    }

  }

  protected def processTimerStarted(event: HistoryEvent) = {
    val attribs = event.getTimerStartedEventAttributes

    // Save the timer so we can look it up when it's fired
    timers += (attribs.getTimerId -> attribs.getControl)

    val timerParams:JsObject = Json.parse(attribs.getControl).as[JsObject]
    val sectionName = timerParams.value("section").as[String]

    val section = getSectionByName(sectionName)

    if(section.status == SectionStatus.DEFERRED) {
      section.timeline.error(s"$sectionName is already DEFERRED!!", Some(new DateTime(event.getEventTimestamp)))
    }

    val reason = timerParams.value("reason").as[String]
    section.setDeferred(reason, new DateTime(event.getEventTimestamp))
    None
  }

  protected def processTimerFired(event: HistoryEvent) = {
    val attribs = event.getTimerFiredEventAttributes

    val timer = timers(attribs.getTimerId)

    val timerParams:JsObject = Json.parse(timer).as[JsObject]
    val sectionName = timerParams.value("section").as[String]
    val section = getSectionByName(sectionName)
    if(section.status == SectionStatus.DEFERRED) {
      section.setStatus(timerParams.value("status").as[String], "Timer fired", new DateTime(event.getEventTimestamp))
    } else {
      section.timeline.warning(s"Timer fired but section status was '${section.status.toString} instead of DEFERRED!", Some(new DateTime(event.getEventTimestamp)))
    }
  }

  protected def processMarkerRecorded(event: HistoryEvent) = {
    val attribs = event.getMarkerRecordedEventAttributes

    val marker = attribs.getMarkerName.split(Constants.delimiter)
    marker(0) match {
      case "OperatorResult" =>
        // Marker is OperatorResult##<section name>##<SUCCESS|FAILURE>
        marker(2) match {
          case "SUCCESS" =>
            getSectionByName(marker(1)).setCompleted(attribs.getDetails, new DateTime(event.getEventTimestamp))
          case _ =>
            getSectionByName(marker(1)).setFailed(marker(2), attribs.getDetails, new DateTime(event.getEventTimestamp))
        }
      case _ =>
        timeline.warning(s"Marker ${attribs.getMarkerName} is unhandled!", None)
    }
  }

  protected def processRecordMarkerFailed(event: HistoryEvent) = {
    val attribs = event.getRecordMarkerFailedEventAttributes
    val marker = attribs.getMarkerName.split(Constants.delimiter)
    marker(0) match {
      case "OperatorResult" =>
        marker(2) match {
          case "SUCCESS" =>
            getSectionByName(marker(1)).timeline.error("Failed to record result for SUCCESSFUL operation "+attribs.getCause, Some(new DateTime(event.getEventTimestamp)))
          case _ =>
            getSectionByName(marker(1)).timeline.error("Failed to record result for FAILED operation "+attribs.getCause, Some(new DateTime(event.getEventTimestamp)))
        }
      case _ =>
        timeline.warning(s"Failed Marker ${attribs.getMarkerName} is unhandled!", None)
    }
  }

  protected def processCancel(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionCanceledEventAttributes
    timeline.warning("CANCELLED: "+attribs.getDetails, Some(new DateTime(event.getEventTimestamp)))
    resolution = "CANCELLED"
  }

  protected def processTimedOut(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionTimedOutEventAttributes
    timeline.error("TIMEOUT: "+attribs.getTimeoutType, Some(new DateTime(event.getEventTimestamp)))
    resolution = "TIMED OUT"
  }

  protected def processTerminated(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionTerminatedEventAttributes
    timeline.error("TERMINATED: "+attribs.getCause+":"+attribs.getReason+":"+attribs.getDetails, Some(new DateTime(event.getEventTimestamp)))
    resolution = "TERMINATED"
  }

  protected def processFailed(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionFailedEventAttributes
    timeline.error("FAILED: "+attribs.getReason+":"+attribs.getDetails, Some(new DateTime(event.getEventTimestamp)))
    resolution = "FAILED"
  }

  protected def processCompleted(event: HistoryEvent) = {
    val attribs = event.getWorkflowExecutionCompletedEventAttributes
    timeline.success("COMPLETED: result is "+attribs.getResult, Some(new DateTime(event.getEventTimestamp)))
    resolution = "COMPLETED"
  }

  def terminal():Boolean = {
    List("CANCELLED", "TIMED OUT", "TERMINATED", "FAILED").contains(resolution)
  }

  override def toString = {
    (for((k, s) <- nameToSection) yield s"$k\t${s.status.toString}").mkString("\n")
  }
}
