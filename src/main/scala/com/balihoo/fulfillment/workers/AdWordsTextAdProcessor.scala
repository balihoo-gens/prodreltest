package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

abstract class AbstractAdWordsTextAdProcessor extends FulfillmentWorker {
  this: AdWordsAdapterComponent
   with SWFAdapterComponent
   with DynamoAdapterComponent
   with TextAdCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    adCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(params("account"))

      val textAd = adCreator.getTextAd(params) match {
        case ad:TextAd =>
          adCreator.updateTextAd(ad, params)
        case _ =>
          adCreator.createTextAd(params)
      }
      completeTask(String.valueOf(textAd.getId))

    } catch {
      case rateExceeded: RateExceededException =>
        // Whoops! We've hit the rate limit! Let's sleep!
        Thread.sleep(rateExceeded.error.getRetryAfterSeconds * 1200) // 120% of the the recommended wait time
        throw rateExceeded
      case exception: Exception =>
        throw exception
      case throwable: Throwable =>
        throw new Exception(throwable.getMessage)
    }
  }
}

class AdWordsTextAdProcessor(swf: SWFAdapter, dyn: DynamoAdapter, awa: AdWordsAdapter)
  extends AbstractAdWordsTextAdProcessor
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with AdWordsAdapterComponent
  with TextAdCreatorComponent {
    //don't put this in the creator method to avoid a new one from
    //being created on every call.
    lazy val _creator = new AdCreator(awa)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def adWordsAdapter = awa
    def adCreator = _creator
}

trait TextAdCreatorComponent {
  def adCreator: AbstractAdCreator with AdWordsAdapterComponent

  abstract class AbstractAdCreator {
    this: AdWordsAdapterComponent =>

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("account", "int", "Participant AdWords account ID"),
        new ActivityParameter("adGroupId", "int", "AdWords AdGroup ID"),
        new ActivityParameter("headline", "string", "Headline of the ad"),
        new ActivityParameter("description1", "string", "First line of ad text"),
        new ActivityParameter("description2", "string", "Second line of ad text"),
        new ActivityParameter("url", "string", "Landing page URL"),
        new ActivityParameter("displayUrl", "string", "Visible Ad URL")
      ))
    }

    def getTextAd(params: ActivityParameters): TextAd = {

      val name = params("headline")
      val adGroupId = params("adGroupId")
      val context = s"getTextAd(name='$name', adGroup='$adGroupId')"

      val selector = new SelectorBuilder()
        .fields("Id", "Url", "DisplayUrl", "Status")
        .equals("TextCreativeName", name)
        .equals("AdGroupId", adGroupId)
        .build()

      adWordsAdapter.withErrorsHandled[TextAd](context, {
        val page = adWordsAdapter.adGroupAdService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0).getAd.asInstanceOf[TextAd]
          case _ => throw new Exception(s"textAd name $name is ambiguous in adGroup '$adGroupId'")
        }
      })
    }

    def newTextAd(params:ActivityParameters): TextAd = {

      val headline = params("headline")
      val desc1 = params("description1")
      val desc2 = params("description2")
      val displayUrl = params("displayUrl")
      val url = params("url")

      val tad = new TextAd()
      tad.setHeadline(headline)
      tad.setDescription1(desc1)
      tad.setDescription2(desc2)
      tad.setDisplayUrl(displayUrl)
      tad.setUrl(url)

      tad
    }

    def createTextAd(params:ActivityParameters): TextAd = {
      _add(newTextAd(params), params)
    }

    def updateTextAd(existingAd:TextAd, params:ActivityParameters): TextAd = {

      val newAd = newTextAd(params)

      if(newAd.equals(existingAd)) {
        // The existing add is exactly the same..
        return existingAd
      }

      _remove(existingAd, params)
      _add(newAd, params)

      newAd
    }

    def _add(tad:TextAd, params:ActivityParameters):TextAd = {
      val aga = new AdGroupAd()
      aga.setAd(tad)
      aga.setAdGroupId(params("adGroupId").toLong)

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.ADD)

      val context = s"Adding a Text Ad $params"

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      }).getAd.asInstanceOf[TextAd]
    }

    def _remove(tad:TextAd, params:ActivityParameters) = {
      val aga = new AdGroupAd()
      aga.setAd(tad)
      aga.setAdGroupId(params("adGroupId").toLong)

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.REMOVE)

      val context = s"Removing a Text Ad $params"

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      })
    }
  }

  class AdCreator(awa: AdWordsAdapter)
  extends AbstractAdCreator
    with AdWordsAdapterComponent {
      def adWordsAdapter = awa
  }
}

object adwords_textadprocessor {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsTextAdProcessor(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new AdWordsAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
