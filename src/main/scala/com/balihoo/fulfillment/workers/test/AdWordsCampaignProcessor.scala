package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

import scala.collection.mutable

object test_adWordsAdapterCampaignCreator {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val creator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "test campaign",
        "channel" : "DISPLAY",
        "budget" : "11",
        "adschedule" : "M,T,S",
        "status" : "PAUSED",
        "startDate" : "20140625",
        "endDate" : "20140701",
        "targetzips" : "83704,83713",
        "biddingStrategy" : "MANUAL_CPM"
      }"""

    val newCampaign = creator.createCampaign(new ActivityParameters(campaignParams))

    println(newCampaign.getId)

  }

}

object test_adWordsAdapterLocationCriterion {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val creator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

    val zipString = "53001,53002,90210"

    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = creator.getCampaign(new ActivityParameters(campaignParams))
    creator.setTargetZips(campaign, zipString)

  }
}

object test_adWordsAdapterSchedule {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val awa = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }
    val creator = new CampaignCreator with AdWordsAdapterComponent { val adWordsAdapter = awa }

    val adWordsAdapter = awa
    adWordsAdapter.setValidateOnly(false)
    adWordsAdapter.setClientId("100-019-2687")

    val scheduleString = "T,Th"

    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = creator.getCampaign(new ActivityParameters(campaignParams))
    creator.setAdSchedule(campaign, scheduleString)

  }
}

object test_adWordsAdapterZipsByCountry {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords")
    val adWordsAdapter = new AdWordsAdapter with PropertiesLoaderComponent { val config = cfg }

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
