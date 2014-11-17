package com.balihoo.fulfillment.deciders

import java.util.UUID.randomUUID
import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._
import scala.collection.mutable
import org.joda.time.DateTime

/**
 *
 * @param fulfillment Fulfillment
 */
class DecisionGenerator(fulfillment: Fulfillment) {

  protected def _createTimerDecision(name:String, delaySeconds:Int, status:String, reason:String) = {

    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.StartTimer)

    val timerParams = mutable.Map[String, String]()
    timerParams("section") = name
    timerParams("status") = status
    timerParams("reason") = reason

    val attribs: StartTimerDecisionAttributes = new StartTimerDecisionAttributes
    attribs.setTimerId(randomUUID().toString)
    attribs.setStartToFireTimeout(delaySeconds.toString)
    attribs.setControl(Json.stringify(Json.toJson(timerParams.toMap)))

    decision.setStartTimerDecisionAttributes(attribs)

    decision
  }

  def _checkComplete(): Option[Decision] = {

    if(fulfillment.categorized.workComplete()) {
      // If we're done then let's just bail here
      fulfillment.timeline.success("Workflow Complete!!!", None)

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.CompleteWorkflowExecution)
      return Some(decision)
    }

    None
  }

  def _checkFailed(): Option[Decision] = {

    val failReasons = mutable.MutableList[String]()

    for(section <- fulfillment.categorized.impossible) {
      if(section.essential || fulfillment.categorized.essentialTotal == 0) {
        val message = s"Section ${section.name} is IMPOSSIBLE!"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    // Loop through the problem sections
    for(section <- fulfillment.categorized.terminal) {
      if(section.essential || fulfillment.categorized.essentialTotal == 0) {
        val message = s"Section ${section.name} is TERMINAL!"
        section.timeline.error(message, Some(DateTime.now))
        failReasons += message
      }
    }

    // Any fail reasons are non-recoverable and ultimately terminal for the workflow. We're going to end it.
    if(failReasons.length > 0) {

      val details: String = failReasons.mkString("\n\t", "\n\t", "\n")
      fulfillment.timeline.error("Workflow FAILED "+details, Some(DateTime.now))
      fulfillment.status = FulfillmentStatus.FAILED

      // TODO. We should cancel the in-progress sections as BEST as we can
      val attribs: FailWorkflowExecutionDecisionAttributes = new FailWorkflowExecutionDecisionAttributes
      attribs.setReason("There are failed sections!")
      attribs.setDetails(details)

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.FailWorkflowExecution)
      decision.setFailWorkflowExecutionDecisionAttributes(attribs)

      return Some(decision)
    }

    None
  }

  /**
   * This route is taken if the workflow receives a cancelation request.
   * We can use this notification to cleanly stop processing.
   * @return
   */
  def _checkCancelRequested():Option[Decision] = {
    if(fulfillment.status != FulfillmentStatus.CANCEL_REQUESTED) { return None }

    val attribs = new CancelWorkflowExecutionDecisionAttributes
    attribs.setDetails("Cancel Requested. Shutting down.")

    val decision = new Decision
    decision.setDecisionType(DecisionType.CancelWorkflowExecution)
    decision.setCancelWorkflowExecutionDecisionAttributes(attribs)

    Some(decision)
  }

  def makeDecisions(): List[Decision] = {

    if(fulfillment.terminal()) {
      fulfillment.timeline.error(s"Workflow is TERMINAL (${fulfillment.status})", None)
      return List()
    }

    val decisions = new mutable.MutableList[Decision]()

    do {

      fulfillment.categorized.categorize()
      decisions.clear()

      _checkCancelRequested() match {
        case d:Some[Decision] => return List(d.get)
        case _ =>
      }

      _checkComplete() match {
        case d:Some[Decision] => return List(d.get)
        case _ =>
      }

      _checkFailed() match {
        case d:Some[Decision] => return List(d.get)
        case _ =>
      }

      for(section <- fulfillment.categorized.ready) {
        val delaySeconds = section.calculateWaitSeconds()
        // Does this task need to be delayed until the waitUntil time?
        if (delaySeconds > 0) {
          decisions += waitDecision(section, delaySeconds)
        } else if(section.action.isDefined) {
          decisions += _createActivityDecision(section)
        }
      }

    } while(fulfillment.categorized.checkPromoted)

    fulfillment.categorized.categorize()

    // Loop through the problem sections
    for(section <- fulfillment.categorized.failed) {
      val message = s"Section failed and is allowed to retry (${section.failedCount} of ${section.failureParams.maxRetries})"
      section.timeline.warning(message, None)
      if(!section.fixable) {
        decisions += _createTimerDecision(section.name, section.failureParams.delaySeconds, SectionStatus.READY.toString,
          message)
      } else {
        section.timeline.warning("Section is marked FIXABLE.", None)
      }
    }

    for(section <- fulfillment.categorized.timedout) {
      val message = s"Section timed out and is allowed to retry (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
      section.timeline.warning(message, None)
      decisions += _createTimerDecision(section.name, section.timeoutParams.delaySeconds, SectionStatus.READY.toString,
        message)
    }

    for(section <- fulfillment.categorized.canceled) {
      val message = s"Section was canceled and is allowed to retry (${section.canceledCount} of ${section.cancelationParams.maxRetries})"
      section.timeline.warning(message, None)
      decisions += _createTimerDecision(section.name, section.cancelationParams.delaySeconds, SectionStatus.READY.toString,
        message)
    }


    if(decisions.length == 0 && !fulfillment.categorized.hasPendingSections) {

      // We aren't making any progress...
      fulfillment.status = FulfillmentStatus.BLOCKED

      if(fulfillment.categorized.terminal.length > 0) {
        fulfillment.timeline.error(
          (for(section <- fulfillment.categorized.terminal) yield section.name)
            .mkString("Terminal Sections:\n\t", "\n\t", ""), None)
      }

      if(fulfillment.categorized.blocked.length > 0) {
        fulfillment.timeline.error(
          (for(section <- fulfillment.categorized.blocked) yield section.name)
            .mkString("Blocked Sections:\n\t", "\n\t", ""), None)
      }

    }

    decisions.toList
  }

  /**
   * Creates a timer decision to delay the execution of the worker.
   * @param section the section describing the activity
   * @param waitSeconds the number of seconds the timer should wait
   * @return the decision
   */
  private def waitDecision(section: FulfillmentSection, waitSeconds: Int): Decision = {
    val message = s"Deferred until ${section.waitUntil.get}"
    _createTimerDecision(section.name, waitSeconds, SectionStatus.READY.toString, message)
  }

  /**
   * Creates a decision to trigger a worker.
   * @param section the section describing the activity
   * @return the decision
   */
  private def _createActivityDecision(section: FulfillmentSection): Decision = {
    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.ScheduleActivityTask)

    val taskList = new TaskList
    taskList.setName(section.action.get.getName+section.action.get.getVersion)

    val attribs: ScheduleActivityTaskDecisionAttributes = new ScheduleActivityTaskDecisionAttributes
    attribs.setActivityType(section.action.get)
    attribs.setInput(Json.stringify(Json.toJson(section.gatherParameters())))
    attribs.setTaskList(taskList)
    attribs.setActivityId(section.getActivityId)

    if(section.startToCloseTimeout.isDefined) attribs.setStartToCloseTimeout(section.startToCloseTimeout.get)
    if(section.scheduleToStartTimeout.isDefined) attribs.setScheduleToStartTimeout(section.scheduleToStartTimeout.get)
    if(section.scheduleToCloseTimeout.isDefined) attribs.setScheduleToCloseTimeout(section.scheduleToCloseTimeout.get)
    if(section.heartbeatTimeout.isDefined) attribs.setHeartbeatTimeout(section.heartbeatTimeout.get)

    decision.setScheduleActivityTaskDecisionAttributes(attribs)

    fulfillment.timeline.note("Scheduling work for: "+section.name, None)

    decision
  }
}
