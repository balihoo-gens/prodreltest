package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

import scala.collection.mutable

abstract class CampaignTest(cfg: PropertiesLoader)
    extends AdWordsAdapterComponent
      with CampaignCreatorComponent {
    private val _awa = new AdWordsAdapter(cfg)
    def adWordsAdapter = _awa
    private val _cc = new CampaignCreator(adWordsAdapter)
    def campaignCreator = _cc

    def run: Unit
}

object adWordsCampaignCreator {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_campaignprocessor")
    val test = new TestCampaignCreator(cfg)
    test.run
  }

  class TestCampaignCreator(cfg: PropertiesLoader) extends CampaignTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityParameters(Map(
         "name" -> "test campaign",
          "channel" -> "DISPLAY",
          "budget" -> "11",
          "adschedule" -> "M,T,S",
          "status" -> "PAUSED",
          "startDate" -> "20140625",
          "endDate" -> "20140701",
          "targetzips" -> "83704,83713",
          "biddingStrategy" -> "MANUAL_CPM"
      ))

      val newCampaign = campaignCreator.createCampaign(campaignParams)
      println(newCampaign.getId)
    }
  }
}

object adWordsLocationCriterion {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_campaignprocessor")
    val test = new TestLocationCriterion(cfg)
    test.run
  }

  class TestLocationCriterion(cfg: PropertiesLoader) extends CampaignTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val zipString = "53001,53002,90210"

      val campaignParams = new ActivityParameters(Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
      campaignCreator.setTargetZips(campaign, zipString)
    }
  }
}

object adWordsSchedule {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_campaignprocessor")
    val test = new TestSchedule(cfg)
    test.run
  }

  class TestSchedule(cfg: PropertiesLoader) extends CampaignTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val scheduleString = "T,Th"

      val campaignParams = new ActivityParameters(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
      campaignCreator.setAdSchedule(campaign, scheduleString)
    }
  }
}

object adWordsZipsByCountry {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_campaignprocessor")
    val test = new TestZipsByCountry(cfg)
    test.run
  }

  class TestZipsByCountry(cfg: PropertiesLoader) extends CampaignTest(cfg) {

    def run = {
      val zipString = "55411,55450" // These match Minnesota AND the Deutschland!
      val selector = new SelectorBuilder()
        .fields(
          "Id",
          "LocationName",
          "CanonicalName",
          "DisplayType",
          "ParentLocations",
          "Reach",
          "CountryCode",
          "TargetingStatus")
        .in("LocationName", zipString.split(","): _*) // Evil scala magic to splat a tuple into a Java variadic
        // Set the locale of the returned location names.
        .equals("Locale", "en")
        .build()

      // Make the get request.
      val locationCriteria = adWordsAdapter.locationService.get(selector)
      for(loc <- locationCriteria) {
        for(ploc <- loc.getLocation.getParentLocations) {
          println("--"+ploc.getLocationName)
        }
        println(loc.getLocation.getLocationName)
        println(loc.getLocation.getDisplayType)
        println(loc.getCanonicalName)
        println(loc.getCountryCode)
        println(loc.getSearchTerm)
        println("------------------")
      }
    }
  }
}
