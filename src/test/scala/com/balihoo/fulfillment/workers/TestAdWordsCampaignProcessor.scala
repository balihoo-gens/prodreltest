package com.balihoo.fulfillment.workers

import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions
import scala.collection.convert.wrapAsJava._
import scala.collection.mutable
import com.amazonaws.services.simpleworkflow.model._

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

/**
 * Example on how to mock up all the layers of the cake pattern
 */
@RunWith(classOf[JUnitRunner])
class TestAdWordsCampaignProcessor extends Specification with Mockito
{
  /**
   * Everything is mocked here, except the AccountCreator
   *  a new AccountCreator is instantiated here on every call
   *  to 'accountCreator'
   */
  class AdWordsCampaignProcessorTest
    extends AbstractAdWordsCampaignProcessor
    with LoggingWorkflowAdapterTestImpl
    with LoggingAdwordsWorkflowAdapter
    with CampaignCreatorComponent {

    /**
     * Mock objects for the LoggingAdwordsWorkflowAdapter mixins
     */
    def adWordsAdapter = mock[AdWordsAdapter]

    /**
     * instantiate a REAL Account creator
     */
    def campaignCreator = new CampaignCreator(adWordsAdapter)
  }

  /**
   * The actual test, using all the Mock objects
   */
  "AdWordsCampaignProcessor" should {
    "intialize properly" in {
      //creates an actual accountcreator with mock adapters
      val creator = new AdWordsCampaignProcessorTest
      creator.name.toString mustEqual "workername"
    }
    "return a valid spec" in {
      val creator = new AdWordsCampaignProcessorTest
      val spec = creator.getSpecification
      spec mustNotEqual null
      val factory = JsonSchemaFactory.byDefault()

      val schema:JsonSchema = factory.getJsonSchema(spec.parameterSchema.as[JsonNode])

      val input =
        Json.parse(""" {
          "account" : "flesh of the tuna",
          "channel" : "DISPLAY",
          "budget" : 34545.0,
"adschedule" : ["34545yay"],
"endDate" : "34545yay",
"name" : "34545yay",
"startDate" : "34545yay",
"targetzips" : ["34545yay"]
          }

        """).as[JsonNode]

      val report = schema.validate(input)

      report.isSuccess
    }
  }
}
