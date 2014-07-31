package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.AdWordsUserInterests
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

abstract class AdGroupTest(cfg: PropertiesLoader)
    extends AdWordsAdapterComponent
      with CampaignCreatorComponent
      with AdGroupCreatorComponent {
    private val _awa = new AdWordsAdapter(cfg)
    def adWordsAdapter = _awa
    private val _cc = new CampaignCreator(adWordsAdapter)
    private val _ac = new AdGroupCreator(adWordsAdapter)
    def campaignCreator = _cc
    def adGroupCreator = _ac

    def run: Unit
}

object test_adwordsAdGroupCreator {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val test = new TestAdGroupCreator(cfg)
    test.run
  }

  class TestAdGroupCreator(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams =
        s"""{
         "name" : "fulfillment Campaign",
          "channel" : "DISPLAY"
        }"""
      val campaign = campaignCreator.getCampaign(new ActivityParameters(campaignParams))

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

      val adGroup = adGroupCreator.getAdGroup(new ActivityParameters(adGroupParams))
      val newAdgroup = adGroupCreator.updateAdGroup(adGroup, new ActivityParameters(adGroupParams))

      println(newAdgroup.getId)
    }
  }
}

object test_adwordsAdGroupSetInterests {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val test = new TestAdGroupSetInterests(cfg)
    test.run
  }

  class TestAdGroupSetInterests(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams =
        s"""{
         "name" : "fulfillment campaign",
          "channel" : "DISPLAY"
        }"""
      val campaign = campaignCreator.getCampaign(new ActivityParameters(campaignParams))

      val adgroupParams =
        s"""{
         "name" : "GROUP A",
          "campaignId" : "${campaign.getId}"
        }"""

      val adgroup = adGroupCreator.getAdGroup(new ActivityParameters(adgroupParams))
      adGroupCreator.addUserInterests(adgroup, Array("Vehicle Shows", "Livestock"))

    }
  }
}

object test_adwordsAdGroupSetKeywords {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val test = new TestAdGroupSetKeywords(cfg)
    test.run
  }

  class TestAdGroupSetKeywords(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams =
        s"""{
         "name" : "fulfillment campaign",
          "channel" : "DISPLAY"
        }"""
      val campaign = campaignCreator.getCampaign(new ActivityParameters(campaignParams))
      val adgroupParams =
        s"""{
         "name" : "GROUP A",
          "campaignId" : "${campaign.getId}"
        }"""

      val adgroup = adGroupCreator.getAdGroup(new ActivityParameters(adgroupParams))

      adGroupCreator.addKeywords(adgroup, Array("tuna", "dressage", "aluminum"))
    }
  }
}
