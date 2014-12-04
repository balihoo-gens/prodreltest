package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.v201409.cm.AdGroup
import play.api.libs.json._

abstract class TextAdTest(cfg: PropertiesLoader)
    extends AdWordsAdapterComponent
      with CampaignCreatorComponent
      with AdGroupCreatorComponent
      with TextAdCreatorComponent {
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

object adWordsAdGroupTextAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_textadprocessor")
    val test = new TestAdGroupTextAd(cfg)
    test.run
  }

  class TestAdGroupTextAd(cfg: PropertiesLoader) extends TextAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | PPC test ( Client ID: 100-019-2687 )

      val campaignParamsJson = Json.obj(
         "name" -> "PPC Campaign",
          "channel" -> "SEARCH"
          ,"account" -> "100-019-2687"
          ,"adschedule" -> "M,T,F"
          ,"budget" -> "123.45"
          ,"startDate" -> "20141010"
          ,"endDate" -> "20161012"
          ,"targetzips" -> "83713,83704"
      )
      val campaignParams = campaignCreator.getSpecification.getParameters(campaignParamsJson)
      val campaign = campaignCreator.getCampaign(campaignParams)
      val adgroupParams = Json.toJson(Map(
         "name" -> "Secondary Market"
        ,"account" -> "100-019-2687",
          "campaignId" -> s"${campaign.getId}",
        "status" -> "ENABLED"
        ,"account" -> "100-019-2687"
        ,"bidDollars" -> "10"
      )).as[JsObject]

      val params = adGroupCreator.getSpecification.getParameters(adgroupParams)

      val adgroup = adGroupCreator.getAdGroup(params) match {
        case ag:AdGroup => ag
        case _ =>
          adGroupCreator.createAdGroup(params)
      }


      val textAdParams = Json.toJson(Map(
        "headline" -> "Dog Face",
        "account" -> "100-019-2687",
        "adGroupId" -> s"${adgroup.getId}",
        "url" -> "http://balihoo.com",
        "displayUrl" ->    "http://balihoo.com",
        "description1" -> "Delicious dog face",
        "description2" -> "wrapped in newspaper"
      )).as[JsObject]

      val textAd = adCreator.createTextAd(adCreator.getSpecification.getParameters(textAdParams))
      println(textAd.getHeadline)

    }
  }
}

object adWordsGetAdGroupTextAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_textadprocessor")
    val test = new TestGetAdGroupTextAd(cfg)
    test.run
  }

  class TestGetAdGroupTextAd(cfg: PropertiesLoader) extends TextAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | PPC test ( Client ID: 100-019-2687 )

      val campaignParamsJson = Json.obj(
        "name" -> "PPC Campaign",
          "channel" -> "SEARCH"
        ,"account" -> "100-019-2687"
      )
      val campaignParams = campaignCreator.getSpecification.getParameters(campaignParamsJson)
      val campaign = campaignCreator.getCampaign(campaignParams)
      val adgroupParams = new ActivityParameters(Map(
        "name" -> "Secondary Market",
        "campaignId" -> s"${campaign.getId}"
      ))
      val adgroup = adGroupCreator.getAdGroup(adgroupParams)

      val textAdParams = new ActivityParameters(Map(
        "headline" -> "Dog Face",
        "adGroupId" -> s"${adgroup.getId}"
      ))
      val ad = adCreator.getTextAd(textAdParams)

      println(ad.toString)
    }
  }
}

object adWordsUpdateAdGroupTextAd {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_textadprocessor")
    val test = new TestUpdateAdGroupTextAd(cfg)
    test.run
  }

  class TestUpdateAdGroupTextAd(cfg: PropertiesLoader) extends TextAdTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | PPC test ( Client ID: 100-019-2687 )

      val campaignParamsJson = Json.obj(
        "name" -> "PPC Campaign",
        "channel" -> "SEARCH"
        ,"account" -> "100-019-2687"
      )
      val campaignParams = campaignCreator.getSpecification.getParameters(campaignParamsJson)
      val campaign = campaignCreator.getCampaign(campaignParams)
      val adgroupParams = Map(
         "name" -> "Secondary Market",
          "campaignId" -> s"${campaign.getId}"
        )

      val adgroup = adGroupCreator.getAdGroup(new ActivityParameters(adgroupParams))

      val textAdParams = Map(
         "headline" -> "Dog Face",
          "adGroupId" -> s"${adgroup.getId}",
          "url" -> "http://balihoo.com",
          "displayUrl" ->    "http://balihoo.com",
          "description1" -> "Delicious dog face",
          "description2" -> "wrapped in newspaper"
        )

      val ad = adCreator.getTextAd(new ActivityParameters(textAdParams))

      val textAd = adCreator.updateTextAd(ad, new ActivityParameters(textAdParams))
      println(textAd.getHeadline)
    }
  }
}
