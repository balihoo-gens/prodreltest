package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201406.SelectorBuilder
import com.google.api.ads.adwords.axis.v201406.cm._

import com.balihoo.fulfillment.util.Splogger

abstract class AbstractAdWordsTextAdProcessor extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
   with TextAdCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    adCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    adWordsAdapter.withErrorsHandled[Any]("Text Ad Processor", {
      adWordsAdapter.setClientId(params("account"))

      val textAd = adCreator.getTextAd(params) match {
        case ad:TextAd =>
          adCreator.updateTextAd(ad, params)
        case _ =>
          adCreator.createTextAd(params)
      }
      completeTask(String.valueOf(textAd.getId))

    })
  }
}

class AdWordsTextAdProcessor(override val _cfg:PropertiesLoader, override val _splog:Splogger)
  extends AbstractAdWordsTextAdProcessor
  with LoggingAdwordsWorkflowAdapterImpl
  with TextAdCreatorComponent {
    lazy val _creator = new AdCreator(adWordsAdapter)
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
      ), new ActivityResult("int", "TextAd ID"))
    }

    def getTextAd(params: ActivityParameters): TextAd = {

      val name = params("headline")
      val adGroupId = params("adGroupId")
      val context = s"getTextAd(name='$name', adGroup='$adGroupId')"

      val selector = new SelectorBuilder()
        .fields("Id", "Url", "DisplayUrl", "Status")
        .equals("Headline", name)
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

      val headline = AdWordsPolicy.fixUpperCaseViolations(params("headline"))
      val desc1 = AdWordsPolicy.fixUpperCaseViolations(params("description1"))
      val desc2 = AdWordsPolicy.fixUpperCaseViolations(params("description2"))
      val displayUrl = AdWordsPolicy.noWWW(params("displayUrl"))
      val url = AdWordsPolicy.cleanUrl(params("url"))

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

object adwords_textadprocessor extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsTextAdProcessor(cfg, splog)
  }
}
