package com.balihoo.fulfillment.workers.adwords

import java.net.{URI, URL}

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import com.google.api.ads.adwords.axis.utils.v201409.SelectorBuilder
import com.google.api.ads.adwords.axis.v201409.cm._

abstract class AbstractAdWordsImageAdProcessor extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
   with ImageAdCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    adCreator.getSpecification
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    adWordsAdapter.withErrorsHandled[ActivityResult]("Image Ad Processor", {
      adWordsAdapter.setClientId(args[String]("account"))

      val imageAd = adCreator.getImageAd(args) match {
        case ad:ImageAd =>
          adCreator.updateImageAd(ad, args)
        case _ =>
          adCreator.createImageAd(args)
      }

      getSpecification.createResult(String.valueOf(imageAd.getId))

    })
  }
}

class AdWordsImageAdProcessor(override val _cfg:PropertiesLoader, override val _splog:Splogger)
  extends AbstractAdWordsImageAdProcessor
  with LoggingAdwordsWorkflowAdapterImpl
  with ImageAdCreatorComponent {
    lazy private val _creator = new AdCreator(adWordsAdapter)
    def adCreator = _creator
}

trait ImageAdCreatorComponent {
  def adCreator: AbstractAdCreator with AdWordsAdapterComponent

  abstract class AbstractAdCreator {
    this: AdWordsAdapterComponent =>

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new StringParameter("account", "Participant AdWords account ID"),
        new StringParameter("name", "Name of this ad (used to reference the ad later)"),
        new StringParameter("adGroupId", "AdWords AdGroup ID"),
        new UriParameter("url", "Landing page URL"),
        new UriParameter("displayUrl", "Visible Ad URL"),
        new UriParameter("imageUrl", "URL Location of image data for this ad"),
        new EnumParameter("status", "Enabled by default", List("ENABLED", "PAUSED", "DISABLED"), false)
      ), new StringResultType("ImageAd ID"),
      "Create a Google AdWords Image Ad.\nhttps://developers.google.com/adwords/api/docs/reference/v201409/AdGroupAdService.ImageAd\nhttps://developers.google.com/adwords/api/docs/appendix/limits#ad")
    }

    def getImageAd(params: ActivityArgs): ImageAd = {

      val name = params[String]("name")
      val adGroupId = params[String]("adGroupId")
      val context = s"getImageAd(name='$name', adGroup='$adGroupId')"

      val selector = new SelectorBuilder()
        .fields("Id", "Url", "DisplayUrl", "Status", "MediaId", "ImageCreativeName")
        .equals("ImageCreativeName", name)
        .equals("AdGroupId", adGroupId)
        .build()

      adWordsAdapter.withErrorsHandled[ImageAd](context, {
        val page = adWordsAdapter.adGroupAdService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0).getAd.asInstanceOf[ImageAd]
          case _ => throw new Exception(s"imageAd name $name is ambiguous in adGroup '$adGroupId'")
        }
      })
    }

    def makeImageFromUrl(imageUrl:URL):Image = {
      val bytes =  adWordsAdapter.withErrorsHandled[Array[Byte]]("Fetching image data", {
        com.google.api.ads.common.lib.utils.Media.getMediaDataFromUrl(imageUrl)
      })

      val image = new Image()
      image.setData(bytes)
      image.setType(MediaMediaType.IMAGE)
      image
    }


    def newImageAd(params:ActivityArgs): ImageAd = {

      val name = params[String]("name")
      val url = AdWordsPolicy.destinationUrl(params[URI]("url").toString)
      val displayUrl = AdWordsPolicy.displayUrl(params[URI]("displayUrl").toString)
      val imageUrl = params[URI]("imageUrl").toURL

      AdWordsPolicy.matchDomains(url, displayUrl)

      val image = makeImageFromUrl(imageUrl)

      val ad = new ImageAd()
      ad.setImage(image)
      ad.setName(name)
      ad.setDisplayUrl(displayUrl)
      ad.setUrl(url)

      ad
    }

    def createImageAd(params:ActivityArgs): ImageAd = {
      _add(newImageAd(params), params)
    }

    def updateImageAd(existingAd:ImageAd, args:ActivityArgs): ImageAd = {

      val url = AdWordsPolicy.destinationUrl(args[URI]("url").toString)
      val displayUrl = AdWordsPolicy.displayUrl(args[URI]("displayUrl").toString)

      if (existingAd.getUrl == url && existingAd.getDisplayUrl == displayUrl) {
        existingAd
      } else {
        existingAd.setUrl(url)
        existingAd.setDisplayUrl(displayUrl)
        _update(existingAd, args)
      }

    }

    def _update(iad:ImageAd, params:ActivityArgs):ImageAd = {
      val ad = new Ad()
      ad.setId(iad.getId)
      ad.setDisplayUrl(iad.getDisplayUrl)
      ad.setUrl(iad.getUrl)

      val aga = new AdGroupAd()
      aga.setAd(ad)
      aga.setAdGroupId(params[Long]("adGroupId"))
      aga.setStatus(AdGroupAdStatus.fromString(params.getOrElse("status", "ENABLED").toUpperCase))

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.SET)

      val context = s"(${operation.getOperator.getValue}) an Image Ad $params"

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      }).getAd match {
        case imageAd:ImageAd =>
          imageAd
        case _ =>
          throw new Exception(s"Expected to get an ImageAd! $context")
      }
    }

    def _add(iad:ImageAd, params:ActivityArgs):ImageAd = {
      val aga = new AdGroupAd()
      aga.setAd(iad)
      aga.setAdGroupId(params[Long]("adGroupId"))
      aga.setStatus(AdGroupAdStatus.fromString(params.getOrElse("status", "ENABLED").toUpperCase))

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.ADD)

      val context = s"(${operation.getOperator.getValue}) an Image Ad $params"

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      }).getAd match {
        case imageAd:ImageAd =>
          imageAd
        case _ =>
          throw new Exception(s"Expected to add an ImageAd! $context")
      }
    }

    def _remove(iad:ImageAd, args:ActivityArgs) = {
      val ad = new Ad()
      ad.setId(iad.getId)

      val aga = new AdGroupAd()
      aga.setAd(ad)
      aga.setAdGroupId(args[Long]("adGroupId"))

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.REMOVE)

      val context = s"Removing an Image Ad $args"

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

object adwords_imageadprocessor extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsImageAdProcessor(cfg, splog)
  }
}

