package com.balihoo.fulfillment

import com.balihoo.fulfillment.util.UTCFormatter
import org.joda.time.DateTime

import scala.collection.convert.wrapAsScala._
import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._

class SWFEvent(eventJson:JsObject) {

  val eventId = get[Int]("eventId")
  val eventTypeString = get[String]("eventType")
  val eventType = EventType.fromValue(eventTypeString)
  val eventTimestamp = get[Long]("eventTimestamp")
  val eventDateTime = new DateTime(get[String]("eventDateTime"))

  def get[T: Reads](key:String):T = {
    (eventJson \ key).asOpt[T] match {
      case value:Some[T] => value.get
      case _ => throw new Exception(s"Expected to find key '$key' in ${Json.stringify(eventJson)}")
    }
  }

  def getOrElse[T: Reads](key:String, default:T):T = {
    (eventJson \ key).asOpt[T] match {
      case value:Some[T] => value.get
      case _ => default
    }
  }

}

object SWFHistoryConvertor {

  def historyToJson(historyEvents: java.util.List[HistoryEvent]):JsArray = {
    Json.toJson(for(event: HistoryEvent <- collectionAsScalaIterable(historyEvents))
      yield _historyEventToJson(event)).asInstanceOf[JsArray]
  }

  def jsonToSWFEvents(swfEvents:JsArray):List[SWFEvent] = {
    (for(swfEvent:JsValue <- swfEvents.value)
      yield new SWFEvent(swfEvent.asInstanceOf[JsObject])).toList
  }

  def jsonToSWFEvents(swfEventsJson:String):List[SWFEvent] = {
    Json.parse(swfEventsJson).asOpt[JsArray] match {
      case swfEvents:Some[JsArray] =>
        jsonToSWFEvents(swfEvents.get)
      case _ =>
        throw new Exception(s"Incoming string is not a valid JSON array!")
    }
  }

  def historyToSWFEvents(historyEvents: java.util.List[HistoryEvent]) = {
    jsonToSWFEvents(historyToJson(historyEvents))
  }

  implicit object longWrites extends Writes[java.lang.Long] {
    override def writes(l:java.lang.Long) = l match {
      case null => JsNull
      case _ =>
        JsNumber(l.toLong)
    }
  }

  implicit object workflowExecutionWrites extends Writes[WorkflowExecution] {
    override def writes(we:WorkflowExecution) = {
      we match {
        case null =>
          JsNull
        case _ =>
          Json.obj(
            "runId" -> we.getRunId
            ,"workflowId" -> we.getWorkflowId
          )
      }
    }
  }

  implicit object stringListWrites extends Writes[java.util.List[String]] {
    override def writes(strings:java.util.List[String]) = Json.arr(collectionAsScalaIterable(strings))
  }

  implicit object taskListWrites extends Writes[TaskList] {
    override def writes(taskList:TaskList)
    = taskList match {
      case null => JsNull
      case _ => JsString(taskList.getName)
    }
  }

  implicit object dateTimeWrites extends Writes[DateTime] {
    override def writes(dateTime:DateTime) = dateTime match {
      case null => JsNull
      case _ =>
        JsString(UTCFormatter.format(dateTime))
    }
  }

  implicit object workflowTypeWrites extends Writes[WorkflowType] {
    override def writes(workflowType:WorkflowType) = workflowType match {
      case null => JsNull
      case _ =>
        Json.obj(
          "name" -> workflowType.getName
          ,"version" -> workflowType.getVersion
        )
    }
  }

  implicit object activityTypeWrites extends Writes[ActivityType] {
    override def writes(activityType:ActivityType) = activityType match {
      case null => JsNull
      case _ =>
        Json.obj(
          "name" -> activityType.getName
          , "version" -> activityType.getVersion
        )
    }
  }

  protected def _historyEventToJson(event:HistoryEvent):JsObject = {

    EventType.fromValue(event.getEventType) match {
      case EventType.WorkflowExecutionStarted =>
        val attribs = event.getWorkflowExecutionStartedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"childPolicy" -> attribs.getChildPolicy
          ,"continuedExecutionRunId" -> attribs.getContinuedExecutionRunId
          ,"executionStartToCloseTimeout" -> attribs.getExecutionStartToCloseTimeout
          ,"input" -> attribs.getInput
          ,"parentInitiatedEventId" -> attribs.getParentInitiatedEventId
          ,"parentWorkflowExecution" -> attribs.getParentWorkflowExecution
          ,"tagList" -> attribs.getTagList
          ,"taskList" -> attribs.getTaskList
          ,"taskStartToCloseTimeout" -> attribs.getTaskStartToCloseTimeout
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.WorkflowExecutionCompleted =>
        val attribs = event.getWorkflowExecutionCompletedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"result" -> attribs.getResult
        )
      case EventType.CompleteWorkflowExecutionFailed =>
        val attribs = event.getCompleteWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.WorkflowExecutionFailed =>
        val attribs = event.getWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"details" -> attribs.getDetails
          ,"reason" -> attribs.getReason
        )
      case EventType.FailWorkflowExecutionFailed =>
        val attribs = event.getFailWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.WorkflowExecutionTimedOut =>
        val attribs = event.getWorkflowExecutionTimedOutEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"childPolicy" -> attribs.getChildPolicy
          ,"timeoutType" -> attribs.getTimeoutType
        )
      case EventType.WorkflowExecutionCanceled =>
        val attribs = event.getWorkflowExecutionCanceledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"details" -> attribs.getDetails
        )
      case EventType.CancelWorkflowExecutionFailed =>
        val attribs = event.getCancelWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.WorkflowExecutionContinuedAsNew =>
        val attribs = event.getWorkflowExecutionContinuedAsNewEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"childPolicy" -> attribs.getChildPolicy
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"executionStartToCloseTimeout" -> attribs.getExecutionStartToCloseTimeout
          ,"input" -> attribs.getInput
          ,"newExecutionRunId" -> attribs.getNewExecutionRunId
          ,"tagList" -> attribs.getTagList
          ,"taskList" -> attribs.getTaskList
          ,"taskStartToCloseTimeout" -> attribs.getTaskStartToCloseTimeout
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ContinueAsNewWorkflowExecutionFailed =>
        val attribs = event.getContinueAsNewWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.WorkflowExecutionTerminated =>
        val attribs = event.getWorkflowExecutionTerminatedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"childPolicy" -> attribs.getChildPolicy
          ,"details" -> attribs.getDetails
          ,"reason" -> attribs.getReason
        )
      case EventType.WorkflowExecutionCancelRequested =>
        val attribs = event.getWorkflowExecutionCancelRequestedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"externalInitiatedEventId" -> attribs.getExternalInitiatedEventId
          ,"externalWorkflowExecution" -> attribs.getExternalWorkflowExecution
        )
      case EventType.DecisionTaskScheduled =>
        val attribs = event.getDecisionTaskScheduledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"startToCloseTimeout" -> attribs.getStartToCloseTimeout
          ,"taskList" -> attribs.getTaskList
        )
      case EventType.DecisionTaskStarted =>
        val attribs = event.getDecisionTaskStartedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"identity" -> attribs.getIdentity
          ,"scheduledEventId" -> attribs.getScheduledEventId
        )
      case EventType.DecisionTaskCompleted =>
        val attribs = event.getDecisionTaskCompletedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"executionContext" -> attribs.getExecutionContext
          ,"scheduledEventId" -> attribs.getScheduledEventId
          ,"startedEventId" -> attribs.getStartedEventId
        )
      case EventType.DecisionTaskTimedOut =>
        val attribs = event.getDecisionTaskTimedOutEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"scheduledEventId" -> attribs.getScheduledEventId
          ,"startedEventId" -> attribs.getStartedEventId
          ,"timeoutType" -> attribs.getTimeoutType
        )
      case EventType.ActivityTaskScheduled =>
        val attribs = event.getActivityTaskScheduledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"activityId" -> attribs.getActivityId
          ,"activityType" -> attribs.getActivityType
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"heartbeatTimeout" -> attribs.getHeartbeatTimeout
          ,"input" -> attribs.getInput
          ,"scheduleToCloseTimeout" -> attribs.getScheduleToCloseTimeout
          ,"scheduleToStartTimeout" -> attribs.getScheduleToStartTimeout
          ,"startToCloseTimeout" -> attribs.getStartToCloseTimeout
          ,"taskList" -> attribs.getTaskList
        )
      case EventType.ActivityTaskStarted =>
        val attribs = event.getActivityTaskStartedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"identity" -> attribs.getIdentity
          ,"scheduledEventId" -> attribs.getScheduledEventId
        )
      case EventType.ActivityTaskCompleted =>
        val attribs = event.getActivityTaskCompletedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"result" -> attribs.getResult
          ,"scheduledEventId" -> attribs.getScheduledEventId
          ,"startedEventId" -> attribs.getStartedEventId
        )
      case EventType.ActivityTaskFailed =>
        val attribs = event.getActivityTaskFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"details" -> attribs.getDetails
          ,"reason" -> attribs.getReason
          ,"scheduledEventId" -> attribs.getScheduledEventId
          ,"startedEventId" -> attribs.getStartedEventId
        )
      case EventType.ActivityTaskTimedOut =>
        val attribs = event.getActivityTaskTimedOutEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"details" -> attribs.getDetails
          ,"scheduledEventId" -> attribs.getScheduledEventId
          ,"startedEventId" -> attribs.getStartedEventId
          ,"timeoutType" -> attribs.getTimeoutType
        )
      case EventType.ActivityTaskCanceled =>
        val attribs = event.getActivityTaskCanceledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"details" -> attribs.getDetails
          ,"startedEventId" -> attribs.getStartedEventId
          ,"scheduledEventId" -> attribs.getScheduledEventId
          ,"latestCancelRequestedEventId" -> attribs.getLatestCancelRequestedEventId
        )
      case EventType.ActivityTaskCancelRequested =>
        val attribs = event.getActivityTaskCancelRequestedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"activityId" -> attribs.getActivityId
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.WorkflowExecutionSignaled =>
        val attribs = event.getWorkflowExecutionSignaledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"externalInitiatedEventId" -> attribs.getExternalInitiatedEventId
          ,"externalWorkflowExecution" -> attribs.getExternalWorkflowExecution
          ,"input" -> attribs.getInput
          ,"signalName" -> attribs.getSignalName
        )
      case EventType.MarkerRecorded =>
        val attribs = event.getMarkerRecordedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"details" -> attribs.getDetails
          ,"markerName" -> attribs.getMarkerName
        )
      case EventType.RecordMarkerFailed =>
        val attribs = event.getRecordMarkerFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"markerName" -> attribs.getMarkerName
        )
      case EventType.TimerStarted =>
        val attribs = event.getTimerStartedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"startToFireTimeout" -> attribs.getStartToFireTimeout
          ,"timerId" -> attribs.getTimerId
        )
      case EventType.TimerFired =>
        val attribs = event.getTimerFiredEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"startedEventId" -> attribs.getStartedEventId
          ,"timerId" -> attribs.getTimerId
        )
      case EventType.TimerCanceled =>
        val attribs = event.getTimerCanceledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"startedEventId" -> attribs.getStartedEventId
          ,"timerId" -> attribs.getTimerId
        )
      case EventType.StartChildWorkflowExecutionInitiated =>
        val attribs = event.getStartChildWorkflowExecutionInitiatedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"childPolicy" -> attribs.getChildPolicy
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"executionStartToCloseTimeout" -> attribs.getExecutionStartToCloseTimeout
          ,"input" -> attribs.getInput
          ,"tagList" -> attribs.getTagList
          ,"taskList" -> attribs.getTaskList
          ,"taskStartToCloseTimeout" -> attribs.getTaskStartToCloseTimeout
          ,"workflowId" -> attribs.getWorkflowId
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ChildWorkflowExecutionStarted =>
        val attribs = event.getChildWorkflowExecutionStartedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ChildWorkflowExecutionCompleted =>
        val attribs = event.getChildWorkflowExecutionCompletedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"result" -> attribs.getResult
          ,"startedEventId" -> attribs.getStartedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ChildWorkflowExecutionFailed =>
        val attribs = event.getChildWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"details" -> attribs.getDetails
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"reason" -> attribs.getReason
          ,"startedEventId" -> attribs.getStartedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ChildWorkflowExecutionTimedOut =>
        val attribs = event.getChildWorkflowExecutionTimedOutEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"startedEventId" -> attribs.getStartedEventId
          ,"timeoutType" -> attribs.getTimeoutType
          ,"workflowExecution" -> attribs.getWorkflowExecution
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ChildWorkflowExecutionCanceled =>
        val attribs = event.getChildWorkflowExecutionCanceledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"details" -> attribs.getDetails
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"startedEventId" -> attribs.getStartedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.ChildWorkflowExecutionTerminated =>
        val attribs = event.getChildWorkflowExecutionTerminatedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"startedEventId" -> attribs.getStartedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
          ,"workflowType" -> attribs.getWorkflowType
        )
      case EventType.SignalExternalWorkflowExecutionInitiated =>
        val attribs = event.getSignalExternalWorkflowExecutionInitiatedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"input" -> attribs.getInput
          ,"runId" -> attribs.getRunId
          ,"signalName" -> attribs.getSignalName
          ,"workflowId" -> attribs.getWorkflowId
        )
      case EventType.ExternalWorkflowExecutionSignaled =>
        val attribs = event.getExternalWorkflowExecutionSignaledEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
        )
      case EventType.SignalExternalWorkflowExecutionFailed =>
        val attribs = event.getSignalExternalWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"runId" -> attribs.getRunId
          ,"workflowId" -> attribs.getWorkflowId
        )
      case EventType.ExternalWorkflowExecutionCancelRequested =>
        val attribs = event.getExternalWorkflowExecutionCancelRequestedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"workflowExecution" -> attribs.getWorkflowExecution
        )
      case EventType.RequestCancelExternalWorkflowExecutionInitiated =>
        val attribs = event.getRequestCancelExternalWorkflowExecutionInitiatedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"runId" -> attribs.getRunId
          ,"workflowId" -> attribs.getWorkflowId
        )
      case EventType.RequestCancelExternalWorkflowExecutionFailed =>
        val attribs = event.getRequestCancelExternalWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"runId" -> attribs.getRunId
          ,"workflowId" -> attribs.getWorkflowId
        )
      case EventType.ScheduleActivityTaskFailed =>
        val attribs = event.getScheduleActivityTaskFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"activityId" -> attribs.getActivityId
          ,"activityType" -> attribs.getActivityType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.RequestCancelActivityTaskFailed =>
        val attribs = event.getRequestCancelActivityTaskFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"activityId" -> attribs.getActivityId
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
        )
      case EventType.StartTimerFailed =>
        val attribs = event.getStartTimerFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"timerId" -> attribs.getTimerId
        )
      case EventType.CancelTimerFailed =>
        val attribs = event.getCancelTimerFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"timerId" -> attribs.getTimerId
        )
      case EventType.StartChildWorkflowExecutionFailed =>
        val attribs = event.getStartChildWorkflowExecutionFailedEventAttributes
        Json.obj(
          "eventId" -> event.getEventId
          ,"eventTimestamp" -> event.getEventTimestamp
          ,"eventDateTime" -> new DateTime(event.getEventTimestamp)
          ,"eventType" -> event.getEventType
          ,"cause" -> attribs.getCause
          ,"control" -> attribs.getControl
          ,"decisionTaskCompletedEventId" -> attribs.getDecisionTaskCompletedEventId
          ,"initiatedEventId" -> attribs.getInitiatedEventId
          ,"workflowId" -> attribs.getWorkflowId
          ,"workflowType" -> attribs.getWorkflowType
        )

      case _ =>
        Json.obj("TYPEUNHANDLED" -> event.getEventType)
    }
  }
}
