package com.balihoo.fulfillment.deciders

import com.amazonaws.services.simpleworkflow.model._
import com.balihoo.fulfillment.workers.PrototypeListProviderWorkerConfig
import com.balihoo.fulfillment.config.WorkflowConfig
import play.api.libs.json._

object PrototypeDeciderConfig extends DeciderBaseConfig {
  val taskList: TaskList = new TaskList()
  taskList.setName(WorkflowConfig.properties.getString("deciderTaskListName"))
}
class PrototypeDecider extends DeciderBase {
  //note: Task ID must only be unique among tasks in the workflow.
  //If there would be more than one, need to give it a different, unique name.
  val activityTaskId: String = "PrototypeListProviderWorkerTaskId"

  def getConfig: DeciderBaseConfig = PrototypeDeciderConfig
  def handleDecisionTaskStarted(dt: DecisionTask) = {

    val history: java.util.List[HistoryEvent] = dt.getEvents
    val previousEvent: HistoryEvent = getPreviousNonDeciderEvent(history)
    val workflowAttributes: WorkflowExecutionStartedEventAttributes = getWorkflowAttributes(history)

    EventType.fromValue(previousEvent.getEventType) match {
      case EventType.WorkflowExecutionStarted =>
        val wfInput = Json.parse(workflowAttributes.getInput)
        val lpInput = wfInput \ "target"
        createActivityTask(dt, lpInput.toString(), PrototypeListProviderWorkerConfig, activityTaskId)
      case EventType.ActivityTaskCompleted =>
        respondWorkflowCompleted(dt, previousEvent.getActivityTaskCompletedEventAttributes.getResult)
      case _ => println("Unhandled event type: " + previousEvent.getEventType)
    }
  }
}


object decider {
  def main(args: Array[String]) {
    val td: PrototypeDecider = new PrototypeDecider()
    td.pollForDeciderTasks()
  }
}
