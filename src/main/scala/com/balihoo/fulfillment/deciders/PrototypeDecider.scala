package com.balihoo.fulfillment.deciders

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.workers.PrototypeListProviderWorkerConfig
import com.balihoo.fulfillment.config.WorkflowConfig

object PrototypeDeciderConfig extends DeciderBaseConfig {
  val taskList: TaskList = new TaskList()
  taskList.setName("test_dt")

  //note: Task ID must only be unique among tasks in the workflow.
  //If there would be more than one, need to give it a different, unique name.
  val activityTaskId: String = "PrototypeListProviderWorkerTaskId"
}

class PrototypeDecider extends DeciderBase {

  def getConfig: DeciderBaseConfig = PrototypeDeciderConfig
  def handleDecisionTaskStarted(dt: DecisionTask) = {

    val history: java.util.List[HistoryEvent] = dt.getEvents
    val previousEvent: Option[HistoryEvent] = getPreviousNonDeciderEvent(history)
    val workflowAttributes: WorkflowExecutionStartedEventAttributes = getWorkflowAttributes(history)

    EventType.fromValue(previousEvent.get.getEventType) match {
      case EventType.WorkflowExecutionStarted =>
        createActivityTask(dt, workflowAttributes.getInput, PrototypeListProviderWorkerConfig, PrototypeDeciderConfig.activityTaskId)
      case EventType.ActivityTaskCompleted =>
        respondWorkflowCompleted(dt, previousEvent.get.getActivityTaskCompletedEventAttributes.getResult)
      case _ => println("Unhandled event type: " + previousEvent.get.getEventType)
    }
  }
}


object decider {
  def main(args: Array[String]) {
    val td: PrototypeDecider = new PrototypeDecider()
    td.pollForDeciderTasks()
  }
}
