package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

abstract class ImageAdTest(cfg: PropertiesLoader)
    extends AdWordsAdapterComponent
      with CampaignCreatorComponent
      with AdGroupCreatorComponent
      with ImageAdCreatorComponent {
    private val _awa = new AdWordsAdapter(cfg)
    def adWordsAdapter = _awa
    private val _cc = new CampaignCreator(adWordsAdapter)
    private val _ac = new AdGroupCreator(adWordsAdapter)
    private val _adc = new AdCreator(adWordsAdapter)
    def campaignCreator = _cc
    def adGroupCreator = _ac
    def adCreator = _adc

    def run: Unit
}

object adWordsGetAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestGetAdGroupImageAd(cfg)
    test.run
  }

  class TestGetAdGroupImageAd(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityParameters(Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
      val adgroupParams = new ActivityParameters(Map(
          "campaignId" -> "${campaign.getId}"
      ))
      val adgroup = adGroupCreator.getAdGroup(adgroupParams)

      val imageAdParams = new ActivityParameters(Map(
         "name" -> "Another Nature",
          "adGroupId" -> "${adgroup.getId}"
      ))
      val ad = adCreator.getImageAd(imageAdParams)

      println(ad.toString)
    }
  }
}

object adWordsAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestAdGroupImageAd(cfg)
    test.run
  }

  class TestAdGroupImageAd(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
      )
      val campaign = campaignCreator.getCampaign(new ActivityParameters(campaignParams))
      val adgroupParams = Map(
         "name" -> "GROUP A",
          "campaignId" -> "${campaign.getId}"
      )
      val adgroup = adGroupCreator.getAdGroup(new ActivityParameters(adgroupParams))

      val imageAdParams = Map(
         "name" -> "Another Nature",
          "adGroupId" -> "${adgroup.getId}",
          "url" -> "http://balihoo.com",
          "displayUrl" ->    "http://balihoo.com",
          "imageUrl" -> "http://lorempixel.com/300/100/nature/"
      )

      adCreator.createImageAd(new ActivityParameters(imageAdParams))
    }
  }
}

object adWordsUpdateAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_imageadprocessor")
    val test = new TestUpdateAdGroupImageAd(cfg)
    test.run
  }

  class TestUpdateAdGroupImageAd(cfg: PropertiesLoader) extends ImageAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
        )
      val campaign = campaignCreator.getCampaign(new ActivityParameters(campaignParams))
      val adgroupParams = Map(
         "name" -> "GROUP A",
          "campaignId" -> s"${campaign.getId}"
        )

      val adgroup = adGroupCreator.getAdGroup(new ActivityParameters(adgroupParams))

      val imageAdParams = Map(
         "name" -> "Another Nature",
          "adGroupId" -> "${adgroup.getId}",
          "url" -> "http://balihoo.com",
          "displayUrl" ->    "http://balihoo.com",
          "imageUrl" -> "http://lorempixel.com/300/100/nature/"
        )

      val ad = adCreator.getImageAd(new ActivityParameters(imageAdParams))

      adCreator.updateImageAd(ad, new ActivityParameters(imageAdParams))
    }
  }
}
