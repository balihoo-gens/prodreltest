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

@RunWith(classOf[JUnitRunner])
class TestAdWordsAccountCreator extends Specification with Mockito
{
  trait MockSWFAdapterComponent extends SWFAdapterComponent {
    def swfAdapter = {
      val _swfAdapter = mock[SWFAdapter]
      val _config = mock[PropertiesLoader]
      _config.getString(anyString) returns "mock"
      _config.getString("name") returns "workername"
      _swfAdapter.domain returns "mockdomain"
      _swfAdapter.config returns _config
      _swfAdapter
    }
  }

  trait MockDynamoAdapterComponent extends DynamoAdapterComponent {
    def dynamoAdapter = mock[DynamoAdapter]
  }

  trait MockAdWordsAdapterComponent extends AdWordsAdapterComponent {
    def adWordsAdapter = mock[AdWordsAdapter]
  }

  class TestAdWordsAccountCreator
    extends AbstractAdWordsAccountCreator
    with MockSWFAdapterComponent
    with MockDynamoAdapterComponent
    with MockAdWordsAdapterComponent
    with AccountCreatorComponent {
      def accountCreator = new AccountCreator(adWordsAdapter)
  }

  "AdWordsAccountCreator" should {
    "intialize properly" in {
      //creates an actual accountcreator with mock adapters
      val creator = new TestAdWordsAccountCreator
      creator.name.toString mustEqual "workername"
    }
  }
}
