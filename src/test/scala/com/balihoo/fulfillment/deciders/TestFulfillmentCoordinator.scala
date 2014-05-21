package com.balihoo.fulfillment.deciders

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

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
                      }
                    },
         "params" : { "cake_batter" : { "section" : "batter" },
				              "cake_pan" : "9\" x 11\"",
				              "bake_time" : "40" },
         "prereqs" : ["heat_oven"],
         "status" : "INCOMPLETE",
         "totally unhandled" : "stuff"
	      }}""")

      val section = new FulfillmentSection("test section", json.as[JsObject].value("cake").as[JsObject])

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

      section.params("cake_batter").asInstanceOf[SectionReference].name mustEqual "batter"
      section.params("cake_pan").asInstanceOf[String] mustEqual "9\" x 11\""
      section.params("bake_time").asInstanceOf[String] mustEqual "40"

      section.status mustEqual SectionStatus.withName("INCOMPLETE")

      section.notes(0) mustEqual "Section totally unhandled unhandled!"

    }
  }


}