package com.balihoo.fulfillment.deciders

import java.util.Date
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

@RunWith(classOf[JUnitRunner])
class TestFulfillmentCoordinator extends Specification with Mockito
{
  "FulfillmentSection" should {
    "be initialized without error" in {

      val json = Json.parse( """{
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
         "status" : "INCOMPLETE",
         "waitUntil" : "2093-07-04T16:04:00-06",
         "totally unhandled" : "stuff"
	      }}""")


      val jso = json.as[JsObject].value("cake").as[JsObject]
      val section = new FulfillmentSection("test section", jso, new Date())

      section.name mustEqual "test section"
      section.prereqs(0) mustEqual "heat_oven"
      section.action.getName mustEqual "bake"
      section.action.getVersion mustEqual "1"
      section.failureParams.maxRetries mustEqual 3
      section.failureParams.delaySeconds mustEqual 100
      section.cancelationParams.maxRetries mustEqual 7
      section.cancelationParams.delaySeconds mustEqual 4535
      section.timeoutParams.maxRetries mustEqual 14
      section.timeoutParams.delaySeconds mustEqual 0

      section.scheduledCount mustEqual 0
      section.failedCount mustEqual 0
      section.timedoutCount mustEqual 0
      section.canceledCount mustEqual 0
      section.startedCount mustEqual 0

      section.startToCloseTimeout mustEqual "tuna sandwich"
      section.waitUntil.get mustEqual new DateTime("2093-07-04T16:04:00-06")

      section.params("cake_batter").asInstanceOf[SectionReferences].sections(0).name mustEqual "batter"
      section.params("cake_pan").asInstanceOf[String] mustEqual "9\" x 11\""
      section.params("bake_time").asInstanceOf[String] mustEqual "40"

      section.status mustEqual SectionStatus.withName("INCOMPLETE")

      section.timeline.events(0).message mustEqual "Section input 'totally unhandled' unhandled!"
    }

    "handle status changes" in {

      val json = Json.parse( """{
         "action" : { "name" : "awesome",
                      "version" : "a zillion",
                      "failure" : {
                          "max" : "1",
                          "delay" : "100"
                      },
                      "cancelation" : {
                          "max" : "2",
                          "delay" : "4535"
                      },
                      "timeout" : {
                          "max" : "3",
                          "delay" : "0"
                      },
                      "startToCloseTimeout" : "tuna sandwich"
                    },
         "params" : { "param1" : "1",
				              "param2" : "2"},
         "prereqs" : ["heat_oven"],
         "status" : "INCOMPLETE"
	      }""").as[JsObject]

      val section = new FulfillmentSection("sectionName", json, new Date())

      section.status mustEqual SectionStatus.INCOMPLETE

      section.startedCount mustEqual 0
      section.setStarted(new Date())

      section.status mustEqual SectionStatus.STARTED

      section.startedCount mustEqual 1

      section.setStarted(new Date())
      section.setStarted(new Date())

      section.startedCount mustEqual 3
      section.status mustEqual SectionStatus.STARTED

      section.setScheduled(new Date())
      section.status mustEqual SectionStatus.SCHEDULED

      section.scheduledCount mustEqual 1
      section.startedCount mustEqual 3 // STILL 3

      section.setCompleted("awesome results", new Date())

      section.value mustEqual "awesome results"
      section.scheduledCount mustEqual 1 // STILL 1
      section.startedCount mustEqual 3 // STILL 3
      section.status mustEqual SectionStatus.COMPLETE

      section.setFailed("reasons", "terrible reasons", new Date())
      section.status mustEqual SectionStatus.FAILED
      section.timeline.events.last.message mustEqual "Failed because:reasons terrible reasons"

      section.setFailed("reasons", "more terrible reasons", new Date())
      section.status mustEqual SectionStatus.TERMINAL
      section.timeline.events.last.message mustEqual "Failed because:reasons more terrible reasons"
    }
  }

  "SectionMap" should {
    "be initialized without error" in {

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
           "status" : "INCOMPLETE",
           "totally unhandled" : "stuff"
	       },
        "batter" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "cake_pan" : "9\" x 11\"",
                        "bake_time" : "40" },
           "prereqs" : [],
           "status" : "INCOMPLETE"
         },
        "heat_oven" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "bake_time" : "40" },
           "prereqs" : [],
           "status" : "INCOMPLETE"
         }
	      }"""

      var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

      val event1:HistoryEvent = new HistoryEvent
      val event1Attribs = new WorkflowExecutionStartedEventAttributes
      event1.setEventType(EventType.WorkflowExecutionStarted)
      event1Attribs.setInput(json)
      event1.setWorkflowExecutionStartedEventAttributes(event1Attribs)
      events += event1

      val event2:HistoryEvent = new HistoryEvent
      val event2Attribs = new ActivityTaskScheduledEventAttributes
      event2.setEventType(EventType.ActivityTaskScheduled)
      event2Attribs.setActivityId("cake##blah")
      event2.setActivityTaskScheduledEventAttributes(event2Attribs)
      events += event2

      val map = new SectionMap(mutableSeqAsJavaList(events))

      map.getClass mustEqual classOf[SectionMap]

      map.nameToSection.size mustEqual 3

    }

    "be angry about sanity" in {

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
           "status" : "INCOMPLETE",
           "totally unhandled" : "stuff"
	       },
        "batter" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "cake_pan" : "9\" x 11\"",
                        "bake_time" : "40" },
           "prereqs" : ["doesnotexist"],
           "status" : "INCOMPLETE"
         },
        "heat_oven" : {
           "action" : { "name" : "bake",
                        "version" : "1"
                      },
           "params" : { "bake_time" : "40" },
           "prereqs" : [],
           "status" : "INCOMPLETE"
         }
	      }"""

      var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

      val event1:HistoryEvent = new HistoryEvent
      val event1Attribs = new WorkflowExecutionStartedEventAttributes
      event1.setEventType(EventType.WorkflowExecutionStarted)
      event1Attribs.setInput(json)
      event1.setWorkflowExecutionStartedEventAttributes(event1Attribs)
      events += event1

      val event2:HistoryEvent = new HistoryEvent
      val event2Attribs = new ActivityTaskStartedEventAttributes
      event2.setEventType(EventType.ActivityTaskStarted)
      event2Attribs.setScheduledEventId(100)
      event2.setActivityTaskStartedEventAttributes(event2Attribs)
      events += event2

      val map = new SectionMap(mutableSeqAsJavaList(events))

      map.getClass mustEqual classOf[SectionMap]

      map.nameToSection.size mustEqual 3

      map.timeline.events(0).message mustEqual "Fulfillment is impossible! Prereq (doesnotexist) for batter does not exist!"
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
         """"status" : "INCOMPLETE"
	      }}"""

      var events: mutable.MutableList[HistoryEvent] = mutable.MutableList[HistoryEvent]()

      val event:HistoryEvent = new HistoryEvent
      val eventAttribs = new WorkflowExecutionStartedEventAttributes
      event.setEventType(EventType.WorkflowExecutionStarted)
      eventAttribs.setInput(json)
      event.setWorkflowExecutionStartedEventAttributes(eventAttribs)
      events += event

      val sections = new SectionMap(mutableSeqAsJavaList(events))
      val categorized = new CategorizedSections(sections)
      val generator = new DecisionGenerator(categorized, sections)
      generator.makeDecisions()
    }

    "schedule work when there's no waitUntil" in {
      val decisions = makeDecisions(None)
      decisions(0).getDecisionType mustEqual(DecisionType.ScheduleActivityTask.toString)
    }

    "schedule work when waitUntil is in the past" in {
      val decisions = makeDecisions(Some(DateTime.now.minusDays(1)))
      decisions(0).getDecisionType mustEqual(DecisionType.ScheduleActivityTask.toString)
    }

    "schedule work when waitUntil is < 1 second in the future" in {
      val decisions = makeDecisions(Some(DateTime.now.plusMillis(998)))
      decisions(0).getDecisionType mustEqual(DecisionType.ScheduleActivityTask.toString)
    }

    "start a timer when waitUntil is > 1 second in the future" in {
      val decisions = makeDecisions(Some(DateTime.now.plusSeconds(30)))
      decisions(0).getDecisionType mustEqual(DecisionType.StartTimer.toString)
    }
  }
}
