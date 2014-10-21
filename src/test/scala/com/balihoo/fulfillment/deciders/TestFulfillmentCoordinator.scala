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
  "FulfillmentSection" should {
    "  be initialized without error" in {

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
         "status" : "READY",
         "waitUntil" : "2093-07-04T16:04:00-06",
         "totally unhandled" : "stuff"
	      }}""")


      val jso = json.as[JsObject].value("cake").as[JsObject]
      val section = new FulfillmentSection("test section", jso, DateTime.now)

      section.name mustEqual "test section"
      section.prereqs(0) mustEqual "heat_oven"
      section.action.get.getName mustEqual "bake"
      section.action.get.getVersion mustEqual "1"
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

      section.startToCloseTimeout mustEqual Some("tuna sandwich")
      section.waitUntil.get mustEqual new DateTime("2093-07-04T16:04:00-06")

      section.params("cake_batter").asInstanceOf[SectionReferences].sections(0).name mustEqual "batter"
      section.params("cake_pan").asInstanceOf[String] mustEqual "9\" x 11\""
      section.params("bake_time").asInstanceOf[String] mustEqual "40"

      section.status mustEqual SectionStatus.withName("READY")

      section.timeline.events(0).message mustEqual "totally unhandled : \"stuff\""
    }

    "  handle status changes" in {

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
         "status" : "READY"
	      }""").as[JsObject]

      val section = new FulfillmentSection("sectionName", json, DateTime.now)

      section.status mustEqual SectionStatus.READY

      section.startedCount mustEqual 0
      section.setStarted(DateTime.now)

      section.status mustEqual SectionStatus.STARTED

      section.startedCount mustEqual 1

      section.setStarted(DateTime.now)
      section.setStarted(DateTime.now)

      section.startedCount mustEqual 3
      section.status mustEqual SectionStatus.STARTED

      section.setScheduled(DateTime.now)
      section.status mustEqual SectionStatus.SCHEDULED

      section.scheduledCount mustEqual 1
      section.startedCount mustEqual 3 // STILL 3

      section.setCompleted("awesome results", DateTime.now)

      section.value mustEqual "awesome results"
      section.scheduledCount mustEqual 1 // STILL 1
      section.startedCount mustEqual 3 // STILL 3
      section.status mustEqual SectionStatus.COMPLETE

      section.setFailed("reasons", "terrible reasons", DateTime.now)
      section.status mustEqual SectionStatus.FAILED
      section.timeline.events.last.message mustEqual "Failed because:reasons terrible reasons"

      section.setFailed("reasons", "more terrible reasons", DateTime.now)
      section.status mustEqual SectionStatus.TERMINAL
      section.timeline.events.last.message mustEqual "Failed too many times! (2 > 1)"
    }
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
      generator.makeDecisions(false)
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

  "FulfillmentOperators" should {
    def generateSections(json:String) = {
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

    "  be upset about missing 'input'" in {
      val sections = generateSections("""{
         "neat" : {
            "action" : "MD5",
            "params" : {},
            "prereqs" : [],
            "status" : "READY"
	          }
	        }""")

      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs = decisions(0).getRecordMarkerDecisionAttributes
      attribs mustNotEqual null

      val wrongattribs = decisions(0).getCompleteWorkflowExecutionDecisionAttributes
      wrongattribs mustEqual null

      attribs.getMarkerName mustEqual "OperatorResult##neat##FAILURE"
      attribs.getDetails mustEqual "input parameter 'input' is REQUIRED!"
    }

    "  evaluate MD5 properly and not freak over an undeclared param" in {
      val sections = generateSections("""{
         "neat" : {
            "action" : "MD5",
            "params" : { "input" : "some trash string not related to tuna at all", "fig" : "newton" },
            "prereqs" : [],
            "status" : "READY"
	          }
	        }""")

      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs = decisions(0).getRecordMarkerDecisionAttributes
      attribs mustNotEqual null

      attribs.getMarkerName mustEqual "OperatorResult##neat##SUCCESS"
      attribs.getDetails mustEqual "E546FF3E618B9579D3D039C11A2FFFCA"

      decisions(1).getDecisionType mustEqual "CompleteWorkflowExecution"

    }

    "  format strings with StringFormat" in {
      val sections = generateSections("""{
         "neat" : {
            "action" : "StringFormat",
            "params" : { "format" : "The {subject} eats {food} when {time}",
                         "subject" : "GIANT",
                         "food" : "tuna flesh",
                         "time" : "whenever the hail he wants..!!",
                         "pointless" : "this won't get used" },
            "prereqs" : [],
            "status" : "READY"
	          }
	        }""")

      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs = decisions(0).getRecordMarkerDecisionAttributes
      attribs mustNotEqual null

      attribs.getMarkerName mustEqual "OperatorResult##neat##SUCCESS"
      attribs.getDetails mustEqual "The GIANT eats tuna flesh when whenever the hail he wants..!!"

      decisions(1).getDecisionType mustEqual "CompleteWorkflowExecution"

    }

    "  reference results from other sections properly" in {
      val sections = generateSections("""{
         "neat" : {
            "action" : "StringFormat",
            "params" : { "format" : "The movie {movie} is {detailedreview}",
                         "movie" : ["taen"],
                         "detailedreview" : "alright. I mean it's oK I guess.",
                         "pointless" : "this won't get used" },
            "prereqs" : [],
            "status" : "READY"
	          },
          "taen" : {
            "value" : "ANIMAL HOUSE",
            "status" : "COMPLETE"
          }
	        }""")

      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs = decisions(0).getRecordMarkerDecisionAttributes
      attribs mustNotEqual null

      attribs.getMarkerName mustEqual "OperatorResult##neat##SUCCESS"
      attribs.getDetails mustEqual "The movie ANIMAL HOUSE is alright. I mean it's oK I guess."

      decisions(1).getDecisionType mustEqual "CompleteWorkflowExecution"

    }

    "  multiple operators should evaluate in series" in {
      val sections = generateSections("""{
         "neat" : {
            "action" : "StringFormat",
            "params" : { "format" : "The movie {movie} is {detailedreview}",
                         "movie" : ["taen"],
                         "detailedreview" : "alright. I mean it's oK I guess.",
                         "pointless" : "this won't get used" },
            "prereqs" : [],
            "status" : "READY"
	          },
          "taen" : {
            "action" : "StringFormat",
            "params" : { "format" : "{firstword} {secondword}",
                         "firstword" : "Under",
                         "secondword" : ["seagal got plump"],
                         "pointless" : "this won't get used" },
            "prereqs" : [],
            "status" : "READY"
          },
          "seagal got plump" : {
            "value" : "SIEGE",
            "status" : "COMPLETE"
          }
	        }""")

      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs0 = decisions(0).getRecordMarkerDecisionAttributes
      attribs0 mustNotEqual null

      attribs0.getMarkerName mustEqual "OperatorResult##taen##SUCCESS"
      attribs0.getDetails mustEqual "Under SIEGE"

      val attribs1 = decisions(1).getRecordMarkerDecisionAttributes
      attribs1 mustNotEqual null

      attribs1.getMarkerName mustEqual "OperatorResult##neat##SUCCESS"
      attribs1.getDetails mustEqual "The movie Under SIEGE is alright. I mean it's oK I guess."

      decisions(2).getDecisionType mustEqual "CompleteWorkflowExecution"
    }


    "  URL Encode strings using URLEncode" in {
      val input = "if ((!flip && flap->flop()) || (*deref::ptr)[7]) { return ~(&address); }"
      val sections = generateSections(
      s"""{
        "HumanFoot": {
            "action": "URLEncode",
            "params": {
                "input": "$input"
            },
            "status" : "READY"
        }
      }"""

      )
      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs0 = decisions(0).getRecordMarkerDecisionAttributes
      attribs0 mustNotEqual null
      attribs0.getMarkerName mustEqual "OperatorResult##HumanFoot##SUCCESS"
      attribs0.getDetails mustEqual URLEncoder.encode(input, "UTF-8")
      decisions(1).getDecisionType mustEqual "CompleteWorkflowExecution"
    }

    "  URL Encode strings from a format" in {
      val soulfood = Map(
        "grits" -> "4# of grits",
        "fish" -> "a rusty bucket of F*I*S*H heads",
        //"door" -> "served on a &*^$#W(*&)@#^*&^ cellar d00r!", //Failed because:FAILURE Illegal group reference
        "door" -> "served on a &*^#W(*&)@#^*&^ cellar d00r!",
        "gravy" -> "with gravy. period."
      )
      val format = "\"{" + soulfood.keys.mkString("}, {") + "}\""
      val items = for { (k,v) <- soulfood } yield s""" "$k" : "$v" """
      val input = s"""{
        "HumanFoot": {
            "action": "URLEncode",
            "params": {
                "input": ["SoulFood"]
            },
            "status" : "READY"
        },
        "SoulFood": {
            "action" : "StringFormat",
            "params" : { "format" : $format,
                         ${items.mkString(",\n")}
            },
            "status" : "READY"
        }
      }"""
      val sections = generateSections(input)
      val generator = new DecisionGenerator(sections)
      val decisions = generator.makeDecisions()
      val attribs0 = decisions(0).getRecordMarkerDecisionAttributes
      attribs0 mustNotEqual null

      val soulcat = soulfood.values.mkString(", ")
      attribs0.getMarkerName mustEqual "OperatorResult##SoulFood##SUCCESS"
      attribs0.getDetails mustEqual soulcat

      val attribs1 = decisions(1).getRecordMarkerDecisionAttributes
      attribs1 mustNotEqual null

      attribs1.getMarkerName mustEqual "OperatorResult##HumanFoot##SUCCESS"
      attribs1.getDetails mustEqual URLEncoder.encode(soulcat, "UTF-8")

      decisions(2).getDecisionType mustEqual "CompleteWorkflowExecution"
    }
  }

  "SectionReference" should {
    "  return simple values" in {
      val result = Json.parse("""{
            "value" : "donkey teeth"
	        }""")

      val reference = new SectionReference("none")

      ReferencePath.isJsonPath("stuff") mustEqual false
      ReferencePath.isJsonPath("stuff/tea") mustEqual true
      ReferencePath.isJsonPath("stuff[0]/") mustEqual true
      ReferencePath.isJsonPath("stuff/monkey[9]") mustEqual true
      ReferencePath.isJsonPath("stuff/monkey[9]/cheese") mustEqual true

      reference.section = Some(new FulfillmentSection("some section", result.as[JsObject], DateTime.now))

      reference.getValue mustEqual "donkey teeth"

    }

    "  parse paths properly" in {
      val reference = new SectionReference("none/first/second[3]/fourth")

      reference.name mustEqual "none"
      reference.path.get.components(0).key.get mustEqual "first"
      reference.path.get.components(1).key.get mustEqual "second"
      reference.path.get.components(2).index.get mustEqual 3
      reference.path.get.components(3).key.get mustEqual "fourth"

    }

    "  return json value from jsobject" in {

      val result = """{
            "first" : { "second" : ["u", "o'brien", "y", { "fourth" : "donkey tooth" } ] }
	        }"""
      val sectionJson = Json.parse(s"""{}""")

      val reference = new SectionReference("none/first/second[3]/fourth")
      reference.section = Some(new FulfillmentSection("some section", sectionJson.as[JsObject], DateTime.now))
      reference.section.get.value = result

      reference.getValue mustEqual "donkey tooth"

      val reference2 = new SectionReference("none/first/second/[1]")
      reference2.section = Some(new FulfillmentSection("some section", sectionJson.as[JsObject], DateTime.now))
      reference2.section.get.value = result

      reference2.getValue mustEqual "o'brien"
    }
  }
}
