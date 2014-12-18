package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.AdWordsUserInterests
import com.balihoo.fulfillment.workers.adwords.{CampaignCreatorComponent, AdGroupCreatorComponent}
import com.google.api.ads.adwords.axis.utils.v201409.SelectorBuilder
import com.google.api.ads.adwords.axis.v201409.cm._
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

object adwordsAdGroupCreator {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_adgroupprocessor")
    val test = new TestAdGroupCreator(cfg)
    test.run
  }

  class TestAdGroupCreator(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaign = campaignCreator.getCampaign(new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY"
      )))

      val target =
        """{\"focus\" : \"interests\",\"interests\" : [\"Beauty & Fitness\",\"Books & Literature\"]}"""

      val groupParams = new ActivityArgs(Map(
        "name" -> "CPM AdGroup",
        "status" -> "ENABLED",
        "campaignId" -> s"${campaign.getId}",
        "bidDollars" -> "6.6",
        "target" -> target

      ))

      val adGroup = adGroupCreator.getAdGroup(groupParams)
      val newAdgroup = adGroupCreator.updateAdGroup(adGroup, groupParams)

      println(newAdgroup.getId)
    }
  }
}

object adwordsAdGroupSetInterests {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_adgroupprocessor")
    val test = new TestAdGroupSetInterests(cfg)
    test.run
  }

  class TestAdGroupSetInterests(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
      "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)

      val adgroupParams = new ActivityArgs(Map(
        "name" -> "GROUP A",
        "campaignId" -> s"${campaign.getId}"

      ))

      val adgroup = adGroupCreator.getAdGroup(adgroupParams)
      adGroupCreator._processInterests(adgroup, Array("Vehicle Shows", "Livestock"))

    }
  }
}

object adwordsAdGroupSetKeywords {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_adgroupprocessor")
    val test = new TestAdGroupSetKeywords(cfg)
    test.run
  }

  class TestAdGroupSetKeywords(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)

      val adgroupParams = new ActivityArgs(Map(
        "name" -> "GROUP A",
        "campaignId" -> s"${campaign.getId}"

      ))
      val adgroup = adGroupCreator.getAdGroup(adgroupParams)

      adGroupCreator._processKeywords(adgroup, Array("alabaster", "dressage", "aluminum"), KeywordMatchType.BROAD)
      adGroupCreator._processKeywords(adgroup, Array("alpaca", "camel", "dromedary"), KeywordMatchType.PHRASE)
      adGroupCreator._processKeywords(adgroup, Array("orange", "apple"), KeywordMatchType.EXACT)
      adGroupCreator._processNegativeKeywords(adgroup, Array("applesauce", "gravel"))
    }
  }
}

object adwordsAdGroupSetBidModifier {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_adgroupprocessor")
    val test = new TestAdGroupSetBM(cfg)
    test.run
  }

  class TestAdGroupSetBM(cfg: PropertiesLoader) extends AdGroupTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)

      val adgroupParams = new ActivityArgs(Map(
        "name" -> "GROUP A",
        "campaignId" -> s"${campaign.getId}"

      ))
      val adgroup = adGroupCreator.getAdGroup(adgroupParams)

      adGroupCreator._processBidModifier(adgroup, "0.35", 30001)
    }
  }
}
