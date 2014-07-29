package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

object test_adWordsAdapterGetAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val ccreator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val acreator = new AdGroupCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val adcreator = new AdCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

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

object test_adWordsAdapterAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val ccreator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val acreator = new AdGroupCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val adcreator = new AdCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

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

object test_adWordsAdapterUpdateAdGroupImageAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val ccreator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val acreator = new AdGroupCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val adcreator = new AdCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

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
