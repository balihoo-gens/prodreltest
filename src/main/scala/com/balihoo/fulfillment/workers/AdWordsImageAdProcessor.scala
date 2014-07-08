package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

class AdWordsImageAdProcessor(swfAdapter: SWFAdapter,
                              dynamoAdapter: DynamoAdapter,
                              adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new AdCreator(adwordsAdapter)

      val aga = creator.getImageAd(params) match {
        case ad:AdGroupAd =>
          creator.updateImageAd(ad, params)
        case _ =>
          creator.createImageAd(params)
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

class AdCreator(adwords:AdWordsAdapter) {

  def getImageAd(params: ActivityParameters): AdGroupAd = {

    val name = params.getRequiredParameter("name")
    val adGroupId = params.getRequiredParameter("adGroupId")
    val context = s"getImageAd(name='$name', adGroup='$adGroupId')"

    val selector = new SelectorBuilder()
      .fields("Id", "Url", "DisplayUrl", "Status")
      .equals("ImageCreativeName", name)
      .equals("AdGroupId", adGroupId)
      .build()

    adwords.withErrorsHandled[AdGroupAd](context, {
      val page = adwords.adGroupAdService.get(selector)
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

    adwords.withErrorsHandled[AdGroupAd](context, {
      adwords.adGroupAdService.mutate(Array(operation)).getValue(0)
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

    adwords.withErrorsHandled[AdGroupAd](context, {
      adwords.adGroupAdService.mutate(Array(operation)).getValue(0)
    })

    createImageAd(params)
  }
}

object adwords_imageadprocessor {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsImageAdProcessor(
      new SWFAdapter(config)
      ,new DynamoAdapter(config)
      ,new AdWordsAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

object test_adwordsGetAdGroupImageAd {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)
    val adcreator = new AdCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaignParams =
      """{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    val imageAdParams =
      s"""{
       "name" : "Another Nature",
        "adGroupId" : "${adgroup.getId}"
      }"""
    val ad = adcreator.getImageAd(new ActivityParameters(imageAdParams))

    println(ad.toString)
  }
}

object test_adwordsAdGroupImageAd {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)
    val adcreator = new AdCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    val imageAdParams =
      s"""{
       "name" : "Another Nature",
        "adGroupId" : "${adgroup.getId}",
        "url" : "http://balihoo.com",
        "displayUrl" :    "http://balihoo.com",
        "imageUrl" : "http://lorempixel.com/300/100/nature/"
      }"""

    adcreator.createImageAd(new ActivityParameters(imageAdParams))

  }
}

object test_adwordsUpdateAdGroupImageAd {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)
    val adcreator = new AdCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    val imageAdParams =
      s"""{
       "name" : "Another Nature",
        "adGroupId" : "${adgroup.getId}",
        "url" : "http://balihoo.com",
        "displayUrl" :    "http://balihoo.com",
        "imageUrl" : "http://lorempixel.com/300/100/nature/"
      }"""

    val ad = adcreator.getImageAd(new ActivityParameters(imageAdParams))

    adcreator.updateImageAd(ad, new ActivityParameters(imageAdParams))

  }
}
