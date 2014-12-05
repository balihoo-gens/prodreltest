package com.balihoo.fulfillment.workers

import java.net.URI

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201409.SelectorBuilder
import com.google.api.ads.adwords.axis.v201409.cm._

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
        new StringActivityParameter("account", "Participant AdWords account ID"),
        new IntegerActivityParameter("adGroupId", "AdWords AdGroup ID"),
        new StringActivityParameter("headline", "Headline of the ad", maxLength=Some(25)),
        new StringActivityParameter("description1", "First line of ad text", maxLength=Some(35)),
        new StringActivityParameter("description2", "Second line of ad text", maxLength=Some(35)),
        new UriActivityParameter("url", "Landing page URL (domain must match displayUrl)"),
        new UriActivityParameter("displayUrl", "Visible Ad URL")
      ), new StringActivityResult("TextAd ID"),
        "Create a Google AdWords Text Ad.\nhttps://developers.google.com/adwords/api/docs/reference/v201409/AdGroupAdService.TextAd\nhttps://developers.google.com/adwords/api/docs/appendix/limits#ad" )
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

      val headline = AdWordsPolicy.limitString(params("headline"), 25)
      val desc1 = AdWordsPolicy.fixUpperCaseViolations(AdWordsPolicy.limitString(params("description1"), 35))
      val desc2 = AdWordsPolicy.fixUpperCaseViolations(AdWordsPolicy.limitString(params("description2"), 35))
      val displayUrl = AdWordsPolicy.displayUrl(params[URI]("displayUrl").toString)
      val url = AdWordsPolicy.destinationUrl(params[URI]("url").toString)

      AdWordsPolicy.matchDomains(url, displayUrl)

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
      aga.setAdGroupId(params[Long]("adGroupId"))

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.ADD)

      val context = s"Adding a Text Ad $params"

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      }).getAd match {
        case textAd:TextAd =>
          textAd
        case _ =>
          throw new Exception(s"Expected to add an TextAd! $context")
      }
    }

    def _remove(tad:TextAd, params:ActivityParameters) = {
      val aga = new AdGroupAd()
      aga.setAd(tad)
      aga.setAdGroupId(params[Long]("adGroupId"))

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.REMOVE)

      val context = s"Removing a Text Ad $params"

      adWordsAdapter.withErrorsHandled[Any](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation))
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
