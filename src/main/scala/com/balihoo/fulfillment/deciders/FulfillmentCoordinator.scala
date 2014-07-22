package com.balihoo.fulfillment.deciders

import java.util.UUID.randomUUID

import scala.language.implicitConversions
import scala.collection.convert.wrapAsJava._
import scala.collection.mutable

import com.balihoo.fulfillment.adapters.{SWFAdapterProvider, SWFAdapter}

import com.amazonaws.services.simpleworkflow.model._
import play.api.libs.json._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Getch

object Constants {
  final val delimiter = "##"
}

class FulfillmentCoordinator {
  this: SWFAdapterProvider =>

  def coordinate() = {

    val domain = swfAdapter.config.getString("domain")
    val taskListName = swfAdapter.config.getString("tasklist")

    val taskList: TaskList = new TaskList()
      .withName(taskListName)

    val taskReq: PollForDecisionTaskRequest = new PollForDecisionTaskRequest()
      .withDomain(domain)
      .withTaskList(taskList)

    var done = false
    val getch = new Getch
    getch.addMapping(Seq("q", "Q", "Exit"), () => {println("\nExiting...\n");done = true})

    getch.doWith {
      while(!done) {
        print(".")
        try {
          val task: DecisionTask = swfAdapter.client.pollForDecisionTask(taskReq)

          if(task.getTaskToken != null) {

            val sections = new SectionMap(task.getEvents)
            val categorized = new CategorizedSections(sections)
            val decisions = new DecisionGenerator(categorized, sections).makeDecisions()

            println(sections.notes.mkString("\n"))

            val response: RespondDecisionTaskCompletedRequest = new RespondDecisionTaskCompletedRequest
            response.setTaskToken(task.getTaskToken)
            response.setDecisions(asJavaCollection(decisions))
            swfAdapter.client.respondDecisionTaskCompleted(response)
          }
        } catch {
          case se: java.net.SocketException =>
          // these happen.. no biggie.
          case e: Exception =>
            println("\n" + e.getMessage)
          case t: Throwable =>
            println("\n" + t.getMessage)
        }
      }
    }
    print("Cleaning up...")
  }
}

/**
 *
 * @param categorized The categorized current state of the segments
 * @param sections A Name -> Section mapping helper
 */
class DecisionGenerator(categorized: CategorizedSections
                       ,sections: SectionMap) {

  protected def gatherParameters(section: FulfillmentSection
                                ,sections: SectionMap) = {

    val params = collection.mutable.Map[String, String]()

    for((name, value) <- section.params) {
      value match {
        case sectionReference: SectionReference =>
          params(name) = sectionReference.getValue(sections)
        case v: String =>
          params(name) = v
        case _ =>
          sections.notes += s"Parameter $name doesn't have a recognizable value $value"
      }
    }

    Json.stringify(Json.toJson(params.toMap))

  }

  protected def _createTimerDecision(name:String, delaySeconds:Int, status:String, reason:String) = {

    val decision: Decision = new Decision
    decision.setDecisionType(DecisionType.StartTimer)

    val timerParams = collection.mutable.Map[String, String]()
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

  def makeDecisions(): collection.mutable.MutableList[Decision] = {

    val decisions = new collection.mutable.MutableList[Decision]()

    val failReasons = mutable.MutableList[String]()

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
      details += (for(section <- categorized.impossible) yield s"${section.name} ${section.notes}").mkString(", ")
      failReasons += details
    }

    // Loop through the problem sections
    for(section <- categorized.failed) {
      if(section.failedCount < section.failureParams.maxRetries) {
        val message = s"Section failed and is allowed to retry (${section.failedCount} of ${section.failureParams.maxRetries})"
        section.notes += message
        decisions += _createTimerDecision(section.name, section.failureParams.delaySeconds, SectionStatus.INCOMPLETE.toString,
          message)
      } else {
        failReasons += s"Section $section FAILED too many times! (${section.failedCount} of ${section.failureParams.maxRetries})"
      }
    }

    for(section <- categorized.timedout) {
      if(section.timedoutCount < section.timeoutParams.maxRetries) {
        val message = s"Section timed out and is allowed to retry (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
        section.notes += message
        decisions += _createTimerDecision(section.name, section.timeoutParams.delaySeconds, SectionStatus.INCOMPLETE.toString,
          message)
      } else {
        failReasons += s"Section $section TIMED OUT too many times! (${section.timedoutCount} of ${section.timeoutParams.maxRetries})"
      }
    }

    for(section <- categorized.canceled) {
      if(section.canceledCount < section.cancelationParams.maxRetries) {
        val message = s"Section was canceled and is allowed to retry (${section.canceledCount} of ${section.cancelationParams.maxRetries})"
        section.notes += message
        decisions += _createTimerDecision(section.name, section.cancelationParams.delaySeconds, SectionStatus.INCOMPLETE.toString,
          message)
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

      sections.notes += "Scheduling work for: "+section.name
    }

    if(decisions.length == 0 && !categorized.hasPendingSections) {
      // We aren't making any progress at all! FAIL

      val decision: Decision = new Decision
      decision.setDecisionType(DecisionType.FailWorkflowExecution)
      val attribs = new FailWorkflowExecutionDecisionAttributes

      sections.notes += "Workflow FAILED:"
      if(categorized.blocked.length > 0) {
        var details: String = "Blocked Sections:"
        details += (for(section <- categorized.blocked) yield section.name).mkString(", ")

        val reason = "There are blocked sections and nothing is in progress!"
        attribs.setReason(reason)
        attribs.setDetails(details)
        sections.notes += reason
        sections.notes += details

      } else {
        var details: String = "Sections:\n"
        details += (for((name, section) <- sections.map) yield section.toString).mkString("\n\n")

        val reason = "FAILING because progress isn't being made!"
        attribs.setReason(reason)
        attribs.setDetails(details)
        sections.notes += reason
        //sections.notes += details
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
    val fc: FulfillmentCoordinator = new FulfillmentCoordinator with SWFAdapterProvider {
      val swfAdapter = new SWFAdapter(config)
    }
    println("Running decider")
    fc.coordinate()
  }
}
