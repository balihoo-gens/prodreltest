package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

abstract class AbstractAdWordsImageAdProcessor extends FulfillmentWorker {
  this: AdWordsAdapterComponent
   with SWFAdapterComponent
   with DynamoAdapterComponent
   with AdCreatorComponent =>

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(params.getRequiredParameter("account"))

      val aga = adCreator.getImageAd(params) match {
        case ad:AdGroupAd =>
          adCreator.updateImageAd(ad, params)
        case _ =>
          adCreator.createImageAd(params)
      }
      completeTask(String.valueOf(aga.getAd.getId))

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
  with AdCreatorComponent {
    //don't put this in the creator method to avoid a new one from
    //being created on every call.
    val _creator = new AdCreator(awa)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def adWordsAdapter = awa
    def adCreator = _creator
}

trait AdCreatorComponent {
  def adCreator: AbstractAdCreator with AdWordsAdapterComponent

  abstract class AbstractAdCreator {
    this: AdWordsAdapterComponent =>

    def getImageAd(params: ActivityParameters): AdGroupAd = {

      val name = params.getRequiredParameter("name")
      val adGroupId = params.getRequiredParameter("adGroupId")
      val context = s"getImageAd(name='$name', adGroup='$adGroupId')"

      val selector = new SelectorBuilder()
        .fields("Id", "Url", "DisplayUrl", "Status")
        .equals("ImageCreativeName", name)
        .equals("AdGroupId", adGroupId)
        .build()

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        val page = adWordsAdapter.adGroupAdService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0)
          case _ => throw new Exception(s"imageAd name $name is ambiguous in adGroup '$adGroupId'")
        }
      })
    }

    def createImageAd(params:ActivityParameters): AdGroupAd = {

      val name = params.getRequiredParameter("name")
      val url = params.getRequiredParameter("url")
      val displayUrl = params.getRequiredParameter("displayUrl")
      val imageUrl = params.getRequiredParameter("imageUrl")
      val adGroupId = params.getRequiredParameter("adGroupId")

      val context = s"createImageAd(name='$name', url='$url', displayUrl='$displayUrl', imageUrl='$imageUrl', adGroupId='$adGroupId')"

      val image = new Image()
      image.setData(
        com.google.api.ads.common.lib.utils.Media.getMediaDataFromUrl(imageUrl))
      image.setType(MediaMediaType.IMAGE)

      val ad = new ImageAd()
      ad.setImage(image)
      ad.setName(name)
      ad.setDisplayUrl(displayUrl)
      ad.setUrl(url)

      val aga = new AdGroupAd()
      aga.setAd(ad)
      aga.setAdGroupId(adGroupId.toLong)

      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.ADD)

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      })
    }


    def updateImageAd(aga:AdGroupAd, params:ActivityParameters): AdGroupAd = {

      val name = params.getRequiredParameter("name")
      val adGroupId = params.getRequiredParameter("adGroupId")

      val context = s"updateImageAd(name='$name', adGroupId='$adGroupId', params='$params')"

      val gad = new Ad()
      gad.setId(aga.getAd.getId)

      aga.setAd(gad)
      val operation = new AdGroupAdOperation()
      operation.setOperand(aga)
      operation.setOperator(Operator.REMOVE)

      adWordsAdapter.withErrorsHandled[AdGroupAd](context, {
        adWordsAdapter.adGroupAdService.mutate(Array(operation)).getValue(0)
      })

      createImageAd(params)
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
