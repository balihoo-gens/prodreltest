package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

abstract class AbstractAdWordsImageAdProcessor extends FulfillmentWorker {
  this: AdWordsAdapterComponent
   with SWFAdapterComponent
   with DynamoAdapterComponent
   with ImageAdCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    adCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(params("account"))

      val imageAd = adCreator.getImageAd(params) match {
        case ad:ImageAd =>
          adCreator.updateImageAd(ad, params)
        case _ =>
          adCreator.createImageAd(params)
      }
      completeTask(String.valueOf(imageAd.getId))

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

class AdWordsImageAdProcessor(swf: SWFAdapter, dyn: DynamoAdapter, awa: AdWordsAdapter)
  extends AbstractAdWordsImageAdProcessor
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with AdWordsAdapterComponent
  with ImageAdCreatorComponent {
    //don't put this in the creator method to avoid a new one from
    //being created on every call.
    lazy val _creator = new AdCreator(awa)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def adWordsAdapter = awa
    def adCreator = _creator
}

trait ImageAdCreatorComponent {
  def adCreator: AbstractAdCreator with AdWordsAdapterComponent

  abstract class AbstractAdCreator {
    this: AdWordsAdapterComponent =>

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("account", "int", "Participant AdWords account ID"),
        new ActivityParameter("name", "string", "Name of this ad (used to reference the ad later)"),
        new ActivityParameter("adGroupId", "int", "AdWords AdGroup ID"),
        new ActivityParameter("url", "string", "Landing page URL"),
        new ActivityParameter("displayUrl", "string", "Visible Ad URL"),
        new ActivityParameter("imageUrl", "string", "URL Location of image data for this ad")
      ))
    }

    def getImageAd(params: ActivityParameters): ImageAd = {

      val name = params("name")
      val adGroupId = params("adGroupId")
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

    def newImageAd(params:ActivityParameters): ImageAd = {

      val name = params("name")
      val url = params("url")
      val displayUrl = params("displayUrl")
      val imageUrl = params("imageUrl")

      val image = new Image()
      image.setData(
        com.google.api.ads.common.lib.utils.Media.getMediaDataFromUrl(imageUrl))
      image.setType(MediaMediaType.IMAGE)

      val ad = new ImageAd()
      ad.setImage(image)
      ad.setName(name)
      ad.setDisplayUrl(displayUrl)
      ad.setUrl(url)

      ad
    }

    def createImageAd(params:ActivityParameters): ImageAd = {
      _add(newImageAd(params), params)
    }

    def updateImageAd(existingAd:ImageAd, params:ActivityParameters): ImageAd = {

      val newAd = newImageAd(params)

      if(newAd.equals(existingAd)) {
        // The existing add is exactly the same..
        return existingAd
      }

      _remove(existingAd, params)
      _add(newAd, params)

      newAd

    }

    def _add(iad:ImageAd, params:ActivityParameters):ImageAd = {
      val aga = new AdGroupAd()
      aga.setAd(iad)
      aga.setAdGroupId(params("adGroupId").toLong)

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.ADD)

      val context = s"Adding a Image Ad $params"

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      }).getAd.asInstanceOf[ImageAd]
    }

    def _remove(iad:ImageAd, params:ActivityParameters) = {
      val aga = new AdGroupAd()
      aga.setAd(iad)
      aga.setAdGroupId(params("adGroupId").toLong)

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.REMOVE)

      val context = s"Removing a Image Ad $params"

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

object adwords_imageadprocessor {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsImageAdProcessor(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new AdWordsAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
