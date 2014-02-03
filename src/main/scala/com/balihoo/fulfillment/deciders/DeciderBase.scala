package com.balihoo.fulfillment.deciders

import scala.Array
import com.amazonaws.services.simpleworkflow.model._
import java.net.SocketException
import java.util
import com.balihoo.fulfillment.config.WorkflowConfig
import com.balihoo.fulfillment.workers.WorkerBaseConfig
import scala.collection.JavaConverters._

/**
 * Required configs for each decider
 */
abstract class DeciderBaseConfig {
  val taskList: TaskList
}

abstract class DeciderBase {
  /*** Abstract ***/
  /**
   * Handle the receipt of a decision task. This should only use internal resources and respond quickly.
   * When an course of action has been decided upon,
   * @param dt The decision task returned from the poll
   */
  def handleDecisionTaskStarted(dt: DecisionTask)

  /**
   * @return A reference to the config used for the decider implementation.
   */
  def getConfig: DeciderBaseConfig

  /*** Common ***/
  val config = WorkflowConfig
  val taskReq: PollForDecisionTaskRequest = new PollForDecisionTaskRequest()
  taskReq.setDomain(config.domain)
  taskReq.setTaskList(getConfig.taskList)

  /*
  Implicitly converts HistoryEvents to ExtendedHistoryEvents whenever I need to call isDeciderEvent.
  Pimp my library pattern - https://coderwall.com/p/k_1jzw
  The implicit def is local to this class, so extending HistoryEvent is scoped!
   */
  implicit def extendHistoryEvent(event: HistoryEvent) = new ExtendedHistoryEvent(event)

  /**
   * Poll for new decision tasks and handle disconnects.  Calls the handleDecisionTaskStarted method
   * of the decision task implementation and does not wait for a response.
   */
  //todo: move everything but this to an actor
  def pollForDeciderTasks() = {
    while(true) {
      try {
        val dt:DecisionTask = config.client.pollForDecisionTask(taskReq)

        if (dt.getWorkflowExecution != null) {
          handleDecisionTaskStarted(dt)
        }
      } catch {
        case e: SocketException => {
          if (e.getMessage.equals("Connection reset")) {
            println("connection reset was caught and ignored")
          } else {
            throw e
          }
        }
        case e: Throwable => println("Unexpected exception " + e)
      }
    }
  }

  /**
   * Gets the last event from the workflow history that wasn't a decision task. That is, the event
   * that triggered this decision task.
   * @param eventHistory The workflow history event list
   * @return The history event object of the last element that wasn't related to a decision task.
   */
  def getPreviousNonDeciderEvent(eventHistory:java.util.List[HistoryEvent]): Option[HistoryEvent] = {
    val scalaHistory = eventHistory.asScala
    scalaHistory.reverse.find(!_.isDeciderEvent)
  }

  /**
   * Gets the attributes of the workflow execution started history event.  This is mostly important because
   * it contains the main input to the workflow.
   * @param eventHistory The workflow history event list
   * @return The WorkflowExecutionStartedEventAttributes object for that history event.
   */
  def getWorkflowAttributes(eventHistory:java.util.List[HistoryEvent]): WorkflowExecutionStartedEventAttributes = {
    val startedEvent: HistoryEvent = eventHistory.get(0) //Assuming the first element is the WorkflowExecutionStarted event
    startedEvent.getWorkflowExecutionStartedEventAttributes
  }

  /**
   * Create an activity task as a result of a decision task
   * @param dt The decision task that spawns the activity task.
   * @param input The input for the activity task.
   * @param workerConfig The worker config that contains information about the type of activity to create
   * @param activityId The ID of the activity to create. Must be unique among running tasks in this workflow
   *                   and must not contain any special characters or the substring "arn".
   */
  def createActivityTask(dt: DecisionTask, input: String, workerConfig: WorkerBaseConfig, activityId: String) = {
    println("creating worker task")

    val taskAttributes: ScheduleActivityTaskDecisionAttributes = new ScheduleActivityTaskDecisionAttributes()
    taskAttributes.setActivityType(workerConfig.activityType)
    taskAttributes.setInput(input)
    taskAttributes.setTaskList(workerConfig.taskList)
    taskAttributes.setActivityId(activityId)

    val decision: Decision = new Decision()
    decision.setDecisionType(DecisionType.ScheduleActivityTask)
    decision.setScheduleActivityTaskDecisionAttributes(taskAttributes)

    val decisions: util.ArrayList[Decision] = new util.ArrayList[Decision]
    decisions.add(decision)

    val response: RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest()
    response.setDecisions(decisions)
    response.setTaskToken(dt.getTaskToken)

    println("responding with " + response)
    config.client.respondDecisionTaskCompleted(response)
  }

  /**
   * Respond that the workflow is completed (no other tasks are needed).
   * @param dt The decision task that initiated this response.
   * @param result The final result information from the workflow execution.
   */
  def respondWorkflowCompleted(dt: DecisionTask, result: String) = {

    println("begin respond workflow completed")
    val completionAttributes:CompleteWorkflowExecutionDecisionAttributes = new CompleteWorkflowExecutionDecisionAttributes
    completionAttributes.setResult(result)

    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.CompleteWorkflowExecution)
    decision.setCompleteWorkflowExecutionDecisionAttributes(completionAttributes)

    val decisions: util.ArrayList[Decision] = new util.ArrayList[Decision]
    decisions.add(decision)

    val response: RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest
    response.setDecisions(decisions)
    response.setTaskToken(dt.getTaskToken)

    println("responding decision task complete with " + result)
    config.client.respondDecisionTaskCompleted(response)
  }
}

class ExtendedHistoryEvent(event: HistoryEvent) {
  private val decisionEventTypes = Array(
    EventType.DecisionTaskCompleted,
    EventType.DecisionTaskScheduled,
    EventType.DecisionTaskStarted,
    EventType.DecisionTaskTimedOut)

  def isDeciderEvent = decisionEventTypes.contains(EventType.fromValue(event.getEventType))
}
