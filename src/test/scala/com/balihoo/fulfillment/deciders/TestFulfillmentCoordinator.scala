package com.balihoo.fulfillment.deciders

import java.util.Date
import com.balihoo.fulfillment.SWFHistoryConvertor
import org.joda.time.DateTime

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions
import scala.collection.convert.wrapAsJava._
import scala.collection.mutable
import com.amazonaws.services.simpleworkflow.model._
import java.net.URLEncoder

@RunWith(classOf[JUnitRunner])
class TestFulfillmentCoordinator extends Specification with Mockito
{
  def generateFulfillment(json:String) = {
    var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

    val event:HistoryEvent = new HistoryEvent
    val eventAttribs = new WorkflowExecutionStartedEventAttributes
    event.setEventType(EventType.WorkflowExecutionStarted)
    event.setEventId(1)
    event.setEventTimestamp(new Date())
    eventAttribs.setInput(json)
    eventAttribs.setParentInitiatedEventId(2)
    eventAttribs.setTaskList((new TaskList).withName("blah"))
    event.setWorkflowExecutionStartedEventAttributes(eventAttribs)
    events += event

    new Fulfillment(SWFHistoryConvertor.historyToSWFEvents(mutableSeqAsJavaList(events)))
  }

  "SectionMap" should {
    "  be initialized without error" in {

      val json = """{
         "cake" : {
           "action" : { "name" : "bake",
                        "version" : "1",
                        "failure" : {
                            "max" : "3",
                            "delay" : "100"
                        },
                        "cancelation" : {
                            "max" : "7",
                            "delay" : "4535"
                        },
                        "timeout" : {
                            "max" : "14",
                            "delay" : "0"
                        },
                        "startToCloseTimeout" : "tuna sandwich"
                      },
           "params" : { "cake_batter" : [ "batter" ],
                        "cake_pan" : "9\" x 11\"",
                        "bake_time" : "40" },
           "prereqs" : ["heat_oven"],
           "status" : "READY",
           "totally unhandled" : "stuff"
	       },
        "batter" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "cake_pan" : "9\" x 11\"",
                        "bake_time" : "40" },
           "prereqs" : [],
           "status" : "READY"
         },
        "heat_oven" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "bake_time" : "40" },
           "prereqs" : [],
           "status" : "READY"
         }
	      }"""

      var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

      val event1:HistoryEvent = new HistoryEvent
      val event1Attribs = new WorkflowExecutionStartedEventAttributes
      event1.setEventType(EventType.WorkflowExecutionStarted)
      event1.setEventId(1)
      event1.setEventTimestamp(new Date())
      event1Attribs.setInput(json)
      event1Attribs.setParentInitiatedEventId(1)
      event1Attribs.setTaskList((new TaskList).withName("blah"))
      event1.setWorkflowExecutionStartedEventAttributes(event1Attribs)
      events += event1

      val event2:HistoryEvent = new HistoryEvent
      val event2Attribs = new ActivityTaskScheduledEventAttributes
      event2.setEventType(EventType.ActivityTaskScheduled)
      event2.setEventId(2)
      event2.setEventTimestamp(new Date())
      event2Attribs.setActivityId("cake##blah")
      event2.setActivityTaskScheduledEventAttributes(event2Attribs)
      events += event2

      val map = new Fulfillment(SWFHistoryConvertor.historyToSWFEvents(mutableSeqAsJavaList(events)))

      map.getClass mustEqual classOf[Fulfillment]

      map.size mustEqual 3

    }

    "  be angry about sanity" in {

      val json = """{
         "cake" : {
           "action" : { "name" : "bake",
                        "version" : "1",
                        "failure" : {
                            "max" : "3",
                            "delay" : "100"
                        },
                        "cancelation" : {
                            "max" : "7",
                            "delay" : "4535"
                        },
                        "timeout" : {
                            "max" : "14",
                            "delay" : "0"
                        },
                        "startToCloseTimeout" : "tuna sandwich"
                      },
           "params" : { "cake_batter" : [ "batter" ],
                        "cake_pan" : "9\" x 11\"",
                        "bake_time" : "40" },
           "prereqs" : ["heat_oven"],
           "status" : "READY",
           "totally unhandled" : "stuff"
	       },
        "batter" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "cake_pan" : "9\" x 11\"",
                        "bake_time" : "40" },
           "prereqs" : ["doesnotexist"],
           "status" : "READY"
         },
        "heat_oven" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "bake_time" : "40" },
           "prereqs" : [],
           "status" : "READY"
         }
	      }"""

      var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

      val event1:HistoryEvent = new HistoryEvent
      val event1Attribs = new WorkflowExecutionStartedEventAttributes
      event1.setEventType(EventType.WorkflowExecutionStarted)
      event1.setEventId(1)
      event1.setEventTimestamp(new Date())
      event1Attribs.setInput(json)
      event1Attribs.setParentInitiatedEventId(3)
      event1Attribs.setTaskList((new TaskList).withName("blah"))
      event1.setWorkflowExecutionStartedEventAttributes(event1Attribs)
      events += event1

      val event2:HistoryEvent = new HistoryEvent
      val event2Attribs = new ActivityTaskScheduledEventAttributes
      event2.setEventType(EventType.ActivityTaskScheduled)
      event2.setEventId(2)
      event2.setEventTimestamp(new Date())
      event2Attribs.setActivityId("batter")
      event2.setActivityTaskScheduledEventAttributes(event2Attribs)
      events += event2

      val event3:HistoryEvent = new HistoryEvent
      val event3Attribs = new ActivityTaskStartedEventAttributes
      event3.setEventType(EventType.ActivityTaskStarted)
      event3.setEventId(3)
      event3.setEventTimestamp(new Date())
      event3Attribs.setScheduledEventId(2)
      event3.setActivityTaskStartedEventAttributes(event3Attribs)
      events += event3

      var map:Fulfillment = null
      try {
        map = new Fulfillment(SWFHistoryConvertor.historyToSWFEvents(mutableSeqAsJavaList(events)))
      } catch {
        case e:Exception =>
          println(e.getMessage)
      }

      map.getClass mustEqual classOf[Fulfillment]

      map.size mustEqual 3

      map.timeline.events(0).message mustEqual "Problem processing WorkflowExecutionStarted: Fulfillment is impossible! Prereq (doesnotexist) for batter does not exist!"
    }
  }

  "DecisionGenerator" should {
    def makeDecisions(waitUntil: Option[DateTime]) = {
      val waitUntilString = waitUntil match {
        case Some(d) => "\"waitUntil\" : \"" + d + "\","
        case _ => ""
      }

      val json = """{
         "do something neat" : {
         "action" : { "name" : "best action ever",
                      "version" : "1"
                    },
         "params" : {},
         "prereqs" : [],""" +
         waitUntilString +
         """"status" : "READY"
	      }}"""

      var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

      val event:HistoryEvent = new HistoryEvent
      val eventAttribs = new WorkflowExecutionStartedEventAttributes
      event.setEventType(EventType.WorkflowExecutionStarted)
      event.setEventId(1)
      event.setEventTimestamp(new Date())
      eventAttribs.setInput(json)
      eventAttribs.setParentInitiatedEventId(2)
      eventAttribs.setTaskList((new TaskList).withName("blah"))
      event.setWorkflowExecutionStartedEventAttributes(eventAttribs)
      events += event

      val sections = new Fulfillment(SWFHistoryConvertor.historyToSWFEvents(mutableSeqAsJavaList(events)))
      val generator = new DecisionGenerator(sections)
      generator.makeDecisions()
    }

    "  schedule work when there's no waitUntil" in {
      val decisions = makeDecisions(None)
      decisions(0).getDecisionType mustEqual DecisionType.ScheduleActivityTask.toString
    }

    "  schedule work when waitUntil is in the past" in {
      val decisions = makeDecisions(Some(DateTime.now.minusDays(1)))
      decisions(0).getDecisionType mustEqual DecisionType.ScheduleActivityTask.toString
    }

    "  schedule work when waitUntil is < 1 second in the future" in {
      val decisions = makeDecisions(Some(DateTime.now.plusMillis(998)))
      decisions(0).getDecisionType mustEqual DecisionType.ScheduleActivityTask.toString
    }

    "  start a timer when waitUntil is > 1 second in the future" in {
      val decisions = makeDecisions(Some(DateTime.now.plusSeconds(30)))
      decisions(0).getDecisionType mustEqual DecisionType.StartTimer.toString
    }
  }

  "Section References" should {
    "  initialize properly" in {

      val input = s"""{
        "HumanFoot": {
            "action": { "name" : "none", "version" : "none"},
            "params": {
                "one": "stuff",
                "two": ["cellar door", "stork ankles"]
            },
            "status" : "COMPLETE",
            "value" : "whatever"
        },
        "CellarDoor": {
            "action": { "name" : "CellarDoorAction", "version" : "555"},
            "params": {
                "one": "stuff",
                "two": {"<(section)>" : "HumanFoot"}
            },
            "status" : "READY"
        }
      }"""

      val sections = generateFulfillment(input)

      val dg = new DecisionGenerator(sections)
      val decisions = dg.makeDecisions()

      decisions.size mustEqual 1
      decisions(0).getDecisionType mustEqual DecisionType.ScheduleActivityTask.toString
      decisions(0).getScheduleActivityTaskDecisionAttributes.getInput.contains("whatever")
      decisions(0).getScheduleActivityTaskDecisionAttributes.getActivityType.getName mustEqual "CellarDoorAction"

    }

    "  promote contingency to ready" in {

      val input = s"""{
        "HumanFoot": {
            "action": { "name" : "FootAction", "version" : "777"},
            "params": {
                "one": "stuff",
                "two": ["cellar door", "stork ankles"]
            },
            "status" : "CONTINGENT"
        },
        "CellarDoor": {
            "action": { "name" : "CellarDoorAction", "version" : "555"},
            "params": {
                "one": "stuff",
                "two": {"<(section)>" : "HumanFoot"}
            },
            "status" : "READY"
        }
      }"""

      val sections = generateFulfillment(input)
      val dg = new DecisionGenerator(sections)
      val decisions = dg.makeDecisions()

      decisions.size mustEqual 1
      decisions(0).getDecisionType mustEqual DecisionType.ScheduleActivityTask.toString
      decisions(0).getScheduleActivityTaskDecisionAttributes.getInput.contains("stork ankles")
      decisions(0).getScheduleActivityTaskDecisionAttributes.getActivityType.getName mustEqual "FootAction"

    }

    "  recognize impossibility in referenced section" in {

      val input = s"""{
        "HumanFoot": {
            "action": { "name" : "FootAction", "version" : "777"},
            "params": {
                "one": "stuff",
                "two": ["cellar door", "stork ankles"]
            },
            "status" : "TERMINAL"
        },
        "CellarDoor": {
            "action": { "name" : "CellarDoorAction", "version" : "555"},
            "params": {
                "one": "stuff",
                "two": {"<(section)>" : "HumanFoot"}
            },
            "status" : "READY"
        }
      }"""

      val sections = generateFulfillment(input)
      val dg = new DecisionGenerator(sections)
      val decisions = dg.makeDecisions()

      decisions.size mustEqual 1
      decisions(0).getDecisionType mustEqual DecisionType.FailWorkflowExecution.toString
      decisions(0).getFailWorkflowExecutionDecisionAttributes.getReason mustEqual "There are failed sections!"
      decisions(0).getFailWorkflowExecutionDecisionAttributes.getDetails.trim mustEqual """Section CellarDoor is IMPOSSIBLE!
	Section HumanFoot is TERMINAL!"""

    }
  }

  "Foreach Operator" should {

    "  parse properly" in {

      val input = s"""{
        "HumanFoot * foodItem": {
            "action": "StringFormat",
            "params": {
                "format": "You want gravy on that {foodItem}",
                "foodItem": ["cellar door", "stork ankles"]
            },
            "status" : "READY"
        }
      }"""
      val sections = generateFulfillment(input)

      true
    }

  }
}
