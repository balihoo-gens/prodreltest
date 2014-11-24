package com.balihoo.fulfillment.workers

import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json._

import org.junit.runner._

import scala.language.implicitConversions

import com.balihoo.fulfillment.adapters._

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

      val input =
        Json.parse(""" {
          "account" : "flesh of the tuna",
          "channel" : "DISPLAY",
          "budget" : 34545.0,
"adschedule" : ["34545yay"],
"endDate" : "34546767",
"name" : "some name",
"startDate" : "34543465",
"targetzips" : ["34545yay"]
          }

        """)

      spec.validate(input)

      true

    }
  }
}
