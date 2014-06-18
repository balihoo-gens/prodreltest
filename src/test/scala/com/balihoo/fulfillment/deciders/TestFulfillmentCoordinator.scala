package com.balihoo.fulfillment.deciders

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions
import scala.collection.convert.wrapAsScala._
import scala.collection.convert.wrapAsJava._
import scala.collection.mutable
import com.amazonaws.services.simpleworkflow.model.{ActivityTaskStartedEventAttributes, WorkflowExecutionStartedEventAttributes, EventType, HistoryEvent}

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
         "params" : { "cake_batter" : { "section" : "batter" },
				              "cake_pan" : "9\" x 11\"",
				              "bake_time" : "40" },
         "prereqs" : ["heat_oven"],
         "status" : "INCOMPLETE",
         "totally unhandled" : "stuff"
	      }}""")


      //val jso = json.as[JsObject].value("cake").as[JsObject]
      val jso = json.as[JsObject].value("cake").as[JsObject]
      println("jsot: " + jso)
      //val jso = Json.parse(Json.toJson(jsot)).as[JsObject]
      val section = new FulfillmentSection("test section", jso)

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

      section.params("cake_batter").asInstanceOf[SectionReference].sections(0) mustEqual "batter"
      section.params("cake_pan").asInstanceOf[String] mustEqual "9\" x 11\""
      section.params("bake_time").asInstanceOf[String] mustEqual "40"

      section.status mustEqual SectionStatus.withName("INCOMPLETE")

      section.notes(0) mustEqual "Section totally unhandled unhandled!"
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
           "params" : { "cake_batter" : { "section" : "batter" },
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
      val event2Attribs = new ActivityTaskStartedEventAttributes
      event2.setEventType(EventType.ActivityTaskStarted)
      event2Attribs.setScheduledEventId(100)
      event2.setActivityTaskStartedEventAttributes(event2Attribs)
      events += event2

      val map = new SectionMap(mutableSeqAsJavaList(events))

      map.getClass mustEqual classOf[SectionMap]

      map.map.size mustEqual 3

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
           "params" : { "cake_batter" : { "section" : "batter" },
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

      map.map.size mustEqual 3

      map.notes(1) mustEqual "Fulfillment is impossible! Prereq (doesnotexist) for batter does not exist!"
    }
  }
}
