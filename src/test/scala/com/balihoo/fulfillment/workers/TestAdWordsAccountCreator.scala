package com.balihoo.fulfillment.workers

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
class TestAdWordsAccountCreator extends Specification with Mockito
{
  /**
   * Everything is mocked here, except the AccountCreator
   *  a new AccountCreator is instantiated here on every call
   *  to 'accountCreator'
   */
  class AdWordsAccountCreatorTest
    extends AbstractAdWordsAccountCreator
    with LoggingWorkflowAdapterTestImpl
    with LoggingAdwordsWorkflowAdapter
    with AccountCreatorComponent {

      /**
       * Mock objects for the LoggingAdwordsWorkflowAdapter mixins
       */
      def adWordsAdapter = mock[AdWordsAdapter]

    /**
       * instantiate a REAL Account creator
       */
      def accountCreator = new AccountCreator(adWordsAdapter)
  }

  /**
   * The actual test, using all the Mock objects
   */
  "AdWordsAccountCreator" should {
    "intialize properly" in {
      //creates an actual accountcreator with mock adapters
      val creator = new AdWordsAccountCreatorTest
      creator.name.toString mustEqual "workername"
    }
    "return a valid spec" in {
      val creator = new AdWordsAccountCreatorTest
      val spec = creator.getSpecification
      spec mustNotEqual null
    }
  }
}
