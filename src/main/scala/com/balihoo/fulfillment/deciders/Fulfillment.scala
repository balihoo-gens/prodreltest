package com.balihoo.fulfillment.deciders

import com.balihoo.fulfillment.SWFEvent
import org.joda.time.DateTime

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._


object FulfillmentStatus extends Enumeration {
  val IN_PROGRESS = Value("IN_PROGRESS")
  val BLOCKED = Value("BLOCKED")
  val CANCEL_REQUESTED = Value("CANCEL_REQUESTED")

  // These are all SWF Workflow Close Statii
  val COMPLETED = Value("COMPLETED") // SWF STATUS
  val FAILED = Value("FAILED") // SWF STATUS
  val CANCELED = Value("CANCELED") // SWF STATUS
  val TERMINATED = Value("TERMINATED") // SWF STATUS
  val CONTINUTED_AS_NEW = Value("CONTINUTED_AS_NEW") // SWF STATUS
  val TIMED_OUT = Value("TIMED_OUT") // SWF STATUS
}



/**
 * Build and update Fulfillment from a JSONized SWF execution history
 * @param history:List[SWFEvent]
 */
class Fulfillment(val history:List[SWFEvent]) {

  val eventHandlers = collection.mutable.Map[EventType, (SWFEvent) => (Any)]()
  val registry = collection.mutable.Map[Int, String]()
  val nameToSection = collection.mutable.Map[String, FulfillmentSection]()
  val tags = collection.mutable.Map[String, String]()
  val timeline = new Timeline
  val timers = collection.mutable.Map[String, String]()
  var status = FulfillmentStatus.IN_PROGRESS

  _addEventHandler(EventType.WorkflowExecutionStarted, processWorkflowExecutionStarted)
  _addEventHandler(EventType.ActivityTaskScheduled, processActivityTaskScheduled)
  _addEventHandler(EventType.ActivityTaskStarted, processActivityTaskStarted)
  _addEventHandler(EventType.ActivityTaskCompleted, processActivityTaskCompleted)
  _addEventHandler(EventType.ActivityTaskFailed, processActivityTaskFailed)
  _addEventHandler(EventType.ActivityTaskTimedOut, processActivityTaskTimedOut)
  _addEventHandler(EventType.ActivityTaskCanceled, processActivityTaskCanceled)
  _addEventHandler(EventType.WorkflowExecutionSignaled, processWorkflowExecutionSignaled)
  _addEventHandler(EventType.WorkflowExecutionCancelRequested, processWorkflowExecutionCancelRequested)
  _addEventHandler(EventType.ScheduleActivityTaskFailed, processScheduleActivityTaskFailed)
  _addEventHandler(EventType.TimerStarted, processTimerStarted)
  _addEventHandler(EventType.TimerFired, processTimerFired)
  _addEventHandler(EventType.MarkerRecorded, processIgnoredEventType)
  _addEventHandler(EventType.RecordMarkerFailed, processIgnoredEventType)
  _addEventHandler(EventType.WorkflowExecutionCanceled, processCancel)
  _addEventHandler(EventType.WorkflowExecutionTimedOut, processTimedOut)
  _addEventHandler(EventType.WorkflowExecutionTerminated, processTerminated)
  _addEventHandler(EventType.WorkflowExecutionFailed, processFailed)
  _addEventHandler(EventType.WorkflowExecutionCompleted, processCompleted)
  _addEventHandler(EventType.DecisionTaskScheduled, processIgnoredEventType)
  _addEventHandler(EventType.DecisionTaskStarted, processIgnoredEventType)
  _addEventHandler(EventType.DecisionTaskCompleted, processIgnoredEventType)

  try {
    for(event: SWFEvent <- history) {
      processEvent(event)
    }
  } catch {
    case e:Exception =>
      timeline.error(e.getMessage, Some(DateTime.now))
      status = FulfillmentStatus.FAILED
  }

  // Now that all of the JsObjects have been processed our sections have been created and are up to date.
  val categorized = new CategorizedSections(this)

  protected def _addEventHandler(eventType:EventType, handler:(SWFEvent)=>(Any)) = {
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
          val gripe = s"Fulfillment is impossible! $name has a self-referential prereq!"
          section.setImpossible(gripe, when)
          throw new Exception(gripe)
        }
        if(!hasSection(prereq)) {
          val gripe = s"Fulfillment is impossible! Prereq ($prereq) for $name does not exist!"
          section.setImpossible(gripe, when)
          throw new Exception(gripe)
        }
      }
      for((pname, param) <- section.params) {
        if(pname == name) {
          val gripe = s"Fulfillment is impossible! $name has a self-referential parameter!"
          section.setImpossible(gripe, when)
          throw new Exception(gripe)
        }
        // FIXME: This level of sanity checking is more difficult now!
//        param match {
//          case sectionReferences: SectionReferences =>
//            for(sectionRef <- sectionReferences.sections) {
//              if(!hasSection(sectionRef.name)) {
//                val gripe = s"Fulfillment is impossible! Param ($pname -> ${sectionRef.name}) for $name does not exist!"
//                section.setImpossible(gripe, when)
//                throw new Exception(gripe)
//              }
//            }
//          case _ =>
//        }
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
        println(s"No section $name")
        throw new Exception(s"There is no section '$name'", nsee)
      case e:Exception =>
        throw new Exception(s"Error while looking up section '$name'", e)
    }
  }

  private def getSectionById(id:Int): FulfillmentSection = {
    if(!(registry isDefinedAt id)) {
      throw new Exception(s"No section registered for ID='$id'")
    }
    getSectionByName(registry(id))
  }

  def hasSection(name:String): Boolean = {
    nameToSection isDefinedAt name
  }

  def initializeWithInput(fulfillmentInput:JsObject, when:DateTime) = {
    fulfillmentInput.keys contains "sections" match { // This check is temporary for near-term backwards compatibility
      case true =>
        for((jk, jv) <- fulfillmentInput.fields) {
          jk match {
            case "sections" =>
              _processSections(jv.as[JsObject], when)
            case "tags" =>
              val rtags = jv.as[JsObject]
              for((name, value) <- rtags.fields) {
                tags(name) = value.as[String]
              }
            case _ =>
              timeline.warning(s"Fulfillment parameter '$jk' is unexpected!", Some(DateTime.now()))
          }
        }
      case false =>
        _processSections(fulfillmentInput, when)
    }

    ensureSanity(when)
  }

  protected def _processSections(sections:JsObject, when:DateTime) = {
    for((sectionName, section) <- sections.fields) {
      nameToSection(sectionName) = new FulfillmentSection(sectionName, section.as[JsObject], when)
    }
  }

  protected def processEvent(event:SWFEvent) = {
    try {
      eventHandlers.getOrElse(event.eventType, processUnhandledEventType _)(event)
    } catch {
      case e:Exception =>
        timeline.error(s"Problem processing ${event.eventTypeString}: ${e.getMessage}", Some(event.eventDateTime))
    }

  }

  protected def processUnhandledEventType(event:SWFEvent) = {
    timeline.warning(s"Event type:${event.eventTypeString} unhandled.", Some(event.eventDateTime))
  }

  protected def processIgnoredEventType(event:SWFEvent) = {
  }
  /**
   * This method builds all of the sections from the initial input to the workflow
   * @param event JsObject
   */
  protected def processWorkflowExecutionStarted(event: SWFEvent) = {
    val fulfillmentInput =  try {
      Json.parse(event.get[String]("input")).as[JsObject]
    } catch {
      case e:Exception =>
        val message = "CATASTROPHIC ERROR! Input JSON could not be parsed!"
        timeline.error(message, Some(event.eventDateTime))
        throw new Exception(message+e.getMessage)
    }

    initializeWithInput(fulfillmentInput, event.eventDateTime)
  }

  protected def processActivityTaskScheduled(event: SWFEvent) = {
    val activityIdParts = event.get[String]("activityId").split(Constants.delimiter)
    val name = activityIdParts(0)
    registry += (event.eventId -> name)
    getSectionByName(name).setScheduled(event.eventDateTime)
  }

  protected def processActivityTaskStarted(event: SWFEvent) = {
    getSectionById(event.get[Int]("scheduledEventId")).setStarted(event.eventDateTime)
  }

  protected def processActivityTaskCompleted(event: SWFEvent) = {
    getSectionById(event.get[Int]("scheduledEventId")).setCompleted(event.get[String]("result"), event.eventDateTime)
  }

  protected def processActivityTaskFailed(event: SWFEvent) = {
    getSectionById(event.get[Int]("scheduledEventId")).setFailed(event.get[String]("reason"), event.get[String]("details"), event.eventDateTime)
  }

  protected def processActivityTaskTimedOut(event: SWFEvent) = {
    getSectionById(event.get[Int]("scheduledEventId")).setTimedOut(event.get[String]("timeoutType"), event.get[String]("details"), event.eventDateTime)
  }

  protected def processActivityTaskCanceled(event: SWFEvent) = {
    getSectionById(event.get[Int]("scheduledEventId")).setCanceled(event.get[String]("details"), event.eventDateTime)
  }

  protected def processScheduleActivityTaskFailed(event: SWFEvent) = {
    val activityIdParts = event.get[String]("activityId").split(Constants.delimiter)

    val name = activityIdParts(0)

    // FIXME This isn't the typical 'FAILED'. It failed to even get scheduled
    // Not actually sure if this needs to be distinct or not.
    getSectionByName(name).setFailed("Failed to Schedule task!", event.get[String]("cause"), event.eventDateTime)

  }

  protected def processWorkflowExecutionSignaled(event: SWFEvent) = {
    event.get[String]("signalName") match {
      case "sectionUpdates" =>
        val updates = Json.parse(event.get[String]("input")).as[JsObject]
        for((sectionName, iupdate:JsValue) <- updates.fields) {
          val update = iupdate.as[JsObject]
          val section = getSectionByName(sectionName)
          for((updateType, body:JsValue) <- update.fields) {
            updateType match {
              case "params" =>
                val pupdate = Json.stringify(body)
                section.timeline.note(s"Updating params: $pupdate", Some(event.eventDateTime))
                section.jsonInitParams(body.as[JsObject])
              case "status" =>
                val supdate = body.as[String]
                section.timeline.note(s"Updating status: ${section.status} -> $supdate", Some(event.eventDateTime))
                try {
                  section.setStatus(supdate, "sectionUpdate(status)", event.eventDateTime)
                } catch {
                  case nsee:NoSuchElementException =>
                    section.timeline.error(s"Status $supdate is INVALID!", None)
                }
              case "essential" =>
                val eupdate = body.as[Boolean]
                section.timeline.note(s"Updating essential: $eupdate", Some(event.eventDateTime))
                section.essential = eupdate
              case "action" =>
                val aupdate = Json.stringify(body)
                section.timeline.note(s"Updating action: $aupdate", Some(event.eventDateTime))
                section.jsonInitAction(body.as[JsObject])
              case "prereqs" =>
                val pupdate = Json.stringify(body)
                section.timeline.note(s"Updating prereqs: $pupdate", Some(event.eventDateTime))
                section.jsonInitPrereqs(body.as[JsArray])
              case _ =>
            }
          }
        }
      case "ping" =>
        timeline.note("Received ping!", Some(event.eventDateTime))
      case _ =>
        timeline.warning(s"Unhandled signal ${event.get[String]("signalName")} ${event.get[String]("input")}", Some(event.eventDateTime))
    }

  }
  protected def processWorkflowExecutionCancelRequested(event: SWFEvent) = {
    timeline.warning("Cancel Requested: "+event.get[String]("cause"), Some(event.eventDateTime))

    status = FulfillmentStatus.CANCEL_REQUESTED
  }

  protected def processTimerStarted(event: SWFEvent) = {
    // Save the timer so we can look it up when it's fired
    timers += event.get[String]("timerId") -> event.get[String]("control")

    val timerParams:JsObject = Json.parse(event.get[String]("control")).as[JsObject]
    val sectionName = timerParams.value("section").as[String]

    val section = getSectionByName(sectionName)

    if(section.status == SectionStatus.DEFERRED) {
      section.timeline.error(s"$sectionName is already DEFERRED!!", Some(event.eventDateTime))
    }

    val reason = timerParams.value("reason").as[String]
    section.setDeferred(reason, event.eventDateTime)
    None
  }

  protected def processTimerFired(event: SWFEvent) = {
    val timer = timers(event.get[String]("timerId"))

    val timerParams:JsObject = Json.parse(timer).as[JsObject]
    val sectionName = timerParams.value("section").as[String]
    val section = getSectionByName(sectionName)
    if(section.status == SectionStatus.DEFERRED) {
      section.setStatus(timerParams.value("status").as[String], "Timer fired", event.eventDateTime)
    } else {
      section.timeline.warning(s"Timer fired but section status was '${section.status.toString} instead of DEFERRED!", Some(event.eventDateTime))
    }
  }

  /*
  protected def processMarkerRecorded(event: SWFEvent) = {
    val markerName = event.get[String]("markerName")
    val marker = markerName.split(Constants.delimiter)
    marker(0) match {
      case "OperatorResult" =>
        // Marker is OperatorResult##<section name>##<SUCCESS|FAILURE>
        marker(2) match {
          case "SUCCESS" =>
            getSectionByName(marker(1)).setCompleted(event.get[String]("details"), event.eventDateTime)
          case _ =>
            getSectionByName(marker(1)).setFailed(marker(2), event.get[String]("details"), event.eventDateTime)
        }
      case _ =>
        timeline.warning(s"Marker $markerName is unhandled!", None)
    }
  }

  protected def processRecordMarkerFailed(event: SWFEvent) = {
    val markerName = event.get[String]("markerName")
    val marker = markerName.split(Constants.delimiter)
    marker(0) match {
      case "OperatorResult" =>
        marker(2) match {
          case "SUCCESS" =>
            getSectionByName(marker(1)).timeline.error("Failed to record result for SUCCESSFUL operation "+event.get[String]("cause"), Some(event.eventDateTime))
          case _ =>
            getSectionByName(marker(1)).timeline.error("Failed to record result for FAILED operation "+event.get[String]("cause"), Some(event.eventDateTime))
        }
      case _ =>
        timeline.warning(s"Failed Marker $markerName is unhandled!", None)
    }
  } */

  protected def processCancel(event: SWFEvent) = {
    timeline.warning("CANCELLED: "+event.get[String]("details"), Some(event.eventDateTime))
    status = FulfillmentStatus.CANCELED
  }

  protected def processTimedOut(event: SWFEvent) = {
    timeline.error("TIMEOUT: "+event.get[String]("timeoutType"), Some(event.eventDateTime))
    status = FulfillmentStatus.TIMED_OUT
  }

  protected def processTerminated(event: SWFEvent) = {
    timeline.error("TERMINATED: "+event.get[String]("cause")+":"+event.get[String]("reason")+":"+event.get[String]("details"), Some(event.eventDateTime))
    status = FulfillmentStatus.TERMINATED
  }

  protected def processFailed(event: SWFEvent) = {
    timeline.error("FAILED: "+event.get[String]("reason")+":"+event.get[String]("details"), Some(event.eventDateTime))
    status = FulfillmentStatus.FAILED
  }

  protected def processCompleted(event: SWFEvent) = {
    timeline.success("COMPLETED: result is "+event.get[String]("result"), Some(event.eventDateTime))
    status = FulfillmentStatus.COMPLETED
  }

  def terminal():Boolean = {
    List(FulfillmentStatus.CANCELED, FulfillmentStatus.TIMED_OUT, FulfillmentStatus.TERMINATED, FulfillmentStatus.FAILED).contains(status)
  }

  override def toString = {
    (for((k, s) <- nameToSection) yield s"$k\t${s.status.toString}").mkString("\n")
  }

  def toJson:JsObject = {

    val sectionsJson = collection.mutable.Map[String, JsValue]()
    for((name, section:FulfillmentSection) <- nameToSection) {
      sectionsJson(name) = section.toJson
    }


    Json.obj(
      "timeline" -> timeline.toJson,
      "sections" -> Json.toJson(sectionsJson.toMap),
      "status" -> Json.toJson(status.toString)
    )
  }
}
