package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.workers.adwords.CampaignCreatorComponent
import com.google.api.ads.adwords.axis.utils.v201409.SelectorBuilder
import com.google.api.ads.adwords.axis.v201409.cm._

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

      val campaignParams = new ActivityArgs(Map(
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

      val zipString = List("53001", "53002", "90210")

      val campaignParams = new ActivityArgs(Map(
         "name" -> "fulfillment Campaign",
          "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
//      val ops = campaignCreator.targetZipsOps(campaign, zipString)
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

      val scheduleString = List("T", "Th")

      val campaignParams = new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY"
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
//      campaignCreator.adScheduleOps(campaign, scheduleString)
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

object adWordsAddress {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_campaignprocessor")
    val test = new TestAddressStuff(cfg)
    test.run
  }

  class TestAddressStuff(cfg: PropertiesLoader) extends CampaignTest(cfg) {

    def run = {
      val address = new Address()
      address.setStreetAddress("404 South 8th Street")
      address.setCityName("Boise")
      address.setPostalCode("83702")

      val geoLocationSelector = new GeoLocationSelector()
      geoLocationSelector.setAddresses(Array(address))

      val geoLocations = adWordsAdapter.geoLocationService.get(geoLocationSelector)
      for(geoLocation <- geoLocations) {
        val point = geoLocation.getGeoPoint
        println(s"POINT: ${point.getLatitudeInMicroDegrees} ${point.getLongitudeInMicroDegrees}")
      }

      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY"
      ))

      val campaign = campaignCreator.getCampaign(campaignParams)

      val locationExtension = new ActivityArgs(Map(
        "city" -> "Boise",
        "street address" -> "6700 W Fairview Ave",
        "postal code" -> "83704",
        "country code" -> "US"
      ))
      campaignCreator.setLocationExtension(campaign, locationExtension)
    }
  }
}

object adWordsProximity {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_campaignprocessor")
    val test = new TestProximity(cfg)
    test.run
  }

  class TestProximity(cfg: PropertiesLoader) extends CampaignTest(cfg) {

    def run = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("100-019-2687")

      val campaignParams = new ActivityArgs(Map(
        "name" -> "fulfillment Campaign",
        "channel" -> "DISPLAY",
        "proximity" -> new ActivityArgs(Map(
          "lat" -> 33.8090, // Disneyland
          "lon" -> -117.9190,
          "radius" -> 15.0,
          "radiusUnits" -> "MILES"
        ))
      ))
      val campaign = campaignCreator.getCampaign(campaignParams)
      campaignCreator.updateCampaign(campaign, campaignParams)
    }
  }
}
