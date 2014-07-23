package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.AdWordsUserInterests
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

object test_adwordsAdGroupCreator {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val ccreator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val acreator = new AdGroupCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))

    val target =
      """{\"focus\" : \"interests\",\"interests\" : [\"Beauty & Fitness\",\"Books & Literature\"]}"""

    val adGroupParams =
      s"""{
       "name" : "CPM AdGroup",
       "status" : "ENABLED",
        "campaignId" : "${campaign.getId}",
        "bidDollars" : "6.6",
        "target" : "$target"
      }"""

    val adGroup = acreator.getAdGroup(new ActivityParameters(adGroupParams))

//    val newAdgroup = acreator.createAdGroup(new ActivityParameters(adGroupParams))
    val newAdgroup = acreator.updateAdGroup(adGroup, new ActivityParameters(adGroupParams))

    println(newAdgroup.getId)

  }
}

object test_adwordsAdGroupSetInterests {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val ccreator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val acreator = new AdGroupCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))

    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    acreator.addUserInterests(adgroup, Array("Vehicle Shows", "Livestock"))

  }
}

object test_adwordsAdGroupSetKeywords {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val ccreator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }
    val acreator = new AdGroupCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    acreator.addKeywords(adgroup, Array("tuna", "dressage", "aluminum"))

  }
}
