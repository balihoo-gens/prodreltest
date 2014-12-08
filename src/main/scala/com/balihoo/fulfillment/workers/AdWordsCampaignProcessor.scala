package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

import com.google.api.ads.adwords.axis.utils.v201409.SelectorBuilder
import com.google.api.ads.adwords.axis.v201409.cm._
import org.joda.time.DateTime

import scala.collection.mutable

abstract class AbstractAdWordsCampaignProcessor extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
    with CampaignCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    campaignCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    adWordsAdapter.withErrorsHandled[Any]("Campaign Processor", {
      adWordsAdapter.setClientId(params("account"))

      val campaign = campaignCreator.getCampaign(params) match {
        case campaign: Campaign =>
          campaignCreator.updateCampaign(campaign, params)
        case _ =>
          campaignCreator.createCampaign(params)
      }

      completeTask(String.valueOf(campaign.getId))
    })
  }
}

class AdWordsCampaignProcessor(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractAdWordsCampaignProcessor
  with LoggingAdwordsWorkflowAdapterImpl
  with CampaignCreatorComponent {
    lazy val _creator = new CampaignCreator(adWordsAdapter)
    def campaignCreator = _creator
}

trait BudgetCreatorComponent {
  def budgetCreator: AbstractBudgetCreator with AdWordsAdapterComponent

  abstract class AbstractBudgetCreator {
    this: AdWordsAdapterComponent =>

    def getBudget(campaignName: String, amount: Float): Budget = {

      val context = s"getBudget(name='$campaignName')"

      val selector = new SelectorBuilder()
        .fields("BudgetId")
        .equals("BudgetName", campaignName)
        .build()

      adWordsAdapter.withErrorsHandled[Budget](context, {
        val page = adWordsAdapter.budgetService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => createBudget(campaignName, amount)
          case 1 =>
            val existing = page.getEntries(0)
            val microDollars = adWordsAdapter.dollarsToMicros(amount)
            if(existing.getAmount.getMicroAmount != microDollars) {
              updateBudget(existing, microDollars)
            } else {
              existing
            }
          case _ => throw new Exception(s"Budget name $campaignName is ambiguous!")
        }
      })
    }

    def createBudget(campaignName:String, amount:Float): Budget = {

      val budget = new Budget()
      val money = new Money()
      money.setMicroAmount(adWordsAdapter.dollarsToMicros(amount))
      budget.setAmount(money)
      budget.setName(campaignName)
      budget.setDeliveryMethod(BudgetBudgetDeliveryMethod.STANDARD)
      budget.setPeriod(BudgetBudgetPeriod.DAILY)
      //    budget.setIsExplicitlyShared(true)

      val operation = new BudgetOperation()
      operation.setOperand(budget)
      operation.setOperator(Operator.ADD)

      adWordsAdapter.withErrorsHandled[Budget](s"Creating budget ${budget.getName}", {
        adWordsAdapter.budgetService.mutate(Array(operation)).getValue(0)
      })
    }

    def updateBudget(budget:Budget, micros:Long): Budget = {

      val money = new Money()
      money.setMicroAmount(micros)
      budget.setAmount(money)

      val operation = new BudgetOperation()
      operation.setOperand(budget)
      operation.setOperator(Operator.SET)

      adWordsAdapter.withErrorsHandled[Budget](s"Updating budget ${budget.getName}", {
        adWordsAdapter.budgetService.mutate(Array(operation)).getValue(0)
      })
    }
  }

  class BudgetCreator(awa: AdWordsAdapter)
  extends AbstractBudgetCreator
    with AdWordsAdapterComponent {
      def adWordsAdapter = awa
  }
}

trait CampaignCreatorComponent {
  def campaignCreator: AbstractCampaignCreator with AdWordsAdapterComponent

  abstract class AbstractCampaignCreator {
    this: AdWordsAdapterComponent
      with BudgetCreatorComponent =>

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new StringActivityParameter("account", "Participant AdWords account ID"),
        new StringActivityParameter("name", "Name of the Campaign"),
        new EnumActivityParameter("channel", "The advertising channel", List("SEARCH", "DISPLAY", "SHOPPING")),
        new NumberActivityParameter("budget", "The monthly budget"),
        new EnumActivityParameter("status", "Always PAUSED on Campaign creation", List("ACTIVE", "PAUSED", "DELETED"), required=false),
        new DateTimeActivityParameter("startDate", "First day the campaign will run. Ignored on update."),
        new DateTimeActivityParameter("endDate", "Last day the campaign will run"),
        new StringsActivityParameter("targetzips", "An array of zip codes", required=false),
        new ObjectActivityParameter("proximity", "A proximity is an area within a certain radius of a point with the center point being described by a lat/lon pair", properties=List(
          new NumberActivityParameter("lat", "Latitude in micro-degrees"),
          new NumberActivityParameter("lon", "Longitude in micro-degrees"),
          new NumberActivityParameter("radius", "Distance from lat/lon point"),
          new EnumActivityParameter("radiusUnits", "", List("MILES", "KILOMETERS"))
        ), required = false),
        new EnumsActivityParameter("adschedule", "Days of the week to run ads", options=List("Mon","Tue","Wed","Thu","Fri","Sat","Sun")),
        new ObjectActivityParameter("location", "Location Extension information", properties=List(
          new StringActivityParameter("street address", "Street address line 1"),
          new StringActivityParameter("city", "Name of the city"),
          new StringActivityParameter("postal code", "Postal code"),
          new StringActivityParameter("country code", "Country code"),
          new StringActivityParameter("company name", "The name of the company located at the given address.", required=false, maxLength = Some(80), minLength = Some(1)),
          new StringActivityParameter("phone number", "The phone number for the location", required=false)
        ), required = false)
      ), new StringActivityResult("AdWords Campaign ID"))
    }

    def getCampaign(params: ActivityParameters):Campaign = {

      val name = params[String]("name")
      val channel = params[String]("channel")
      val context = s"getCampaign(name='$name', channel='$channel'"

      val selector = new SelectorBuilder()
        .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType", "BiddingStrategyType")
        .equals("Name", name)
        .equals("AdvertisingChannelType", channel)
        .build()

      adWordsAdapter.withErrorsHandled[Campaign](context, {
        val page = adWordsAdapter.campaignService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0)
          case _ => throw new Exception(s"Campaign name $name is ambiguous!")
        }
      })
    }

    def lookupCampaign(id:String):Campaign = {

      val context = s"lookupCampaign(id='$id')"

      val selector = new SelectorBuilder()
        .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType", "BiddingStrategyType")
        .equals("Id", id)
        .build()

      adWordsAdapter.withErrorsHandled[Campaign](context, {
        val page = adWordsAdapter.campaignService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0)
          case _ => throw new Exception(s"Campaign id $id is ambiguous!")
        }
      })
    }

    def createCampaign(params:ActivityParameters):Campaign = {

      val name = params[String]("name")
      val channel = params[String]("channel")
      val context = s"createCampaign(name='$name', channel='$channel')"

      val budgetName = s"$name Budget"
      val budget = budgetCreator.getBudget(budgetName, params[Float]("budget"))

      val campaign = new Campaign()
      campaign.setName(name)
      campaign.setAdvertisingChannelType(AdvertisingChannelType.fromString(channel))

      campaign.setStatus(CampaignStatus.PAUSED)
      campaign.setBudget(budget)

      if(params.has("startDate")) {
        campaign.setStartDate(params[DateTime]("startDate").toString("YYYYMMdd"))
      }

      if(params.has("endDate")) {
        campaign.setEndDate(params[DateTime]("endDate").toString("YYYYMMdd"))
      }

      val cpcBiddingScheme = new ManualCpcBiddingScheme()
      cpcBiddingScheme.setEnhancedCpcEnabled(false)
      val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
      biddingStrategyConfiguration.setBiddingScheme(cpcBiddingScheme)

      campaign.setBiddingStrategyConfiguration(biddingStrategyConfiguration)

      val networkSetting = new NetworkSetting()
      networkSetting.setTargetPartnerSearchNetwork(false)
      AdvertisingChannelType.fromString(channel) match {
        case AdvertisingChannelType.DISPLAY =>
          networkSetting.setTargetGoogleSearch(false)
        case AdvertisingChannelType.SEARCH =>
          networkSetting.setTargetGoogleSearch(true)
          networkSetting.setTargetSearchNetwork(true)
        case AdvertisingChannelType.SHOPPING =>
          networkSetting.setTargetContentNetwork(false)
        case _ =>
          throw new Exception(s"Media channel $channel is not handled! $context")
      }
      campaign.setNetworkSetting(networkSetting)

      // Set options that are not required.
      val geoTarget = new GeoTargetTypeSetting()
      geoTarget.setPositiveGeoTargetType(GeoTargetTypeSettingPositiveGeoTargetType.DONT_CARE)

      campaign.setSettings(Array(geoTarget))

      val operation = new CampaignOperation()
      operation.setOperand(campaign)
      operation.setOperator(Operator.ADD)

      val madeCampaign = adWordsAdapter.withErrorsHandled[Campaign](context, {
        adWordsAdapter.campaignService.mutate(Array(operation)).getValue(0)
      })

      processCampaignCriterion(campaign, params)

      if(params.has("location")) {
        setLocationExtension(campaign, params[ActivityParameters]("location"))
      }
      madeCampaign
    }

    def processCampaignCriterion(campaign:Campaign, params:ActivityParameters) = {

      val existingCriterion =
        if(campaign.getId > 0)
          _getExistingCampaignCriterion(campaign)
        else
          new CampaignCriterionPage()

      val newCampaignCriterionOperations = collection.mutable.ArrayBuffer[CampaignCriterionOperation]()

      if(params.has("targetzips")) {
        newCampaignCriterionOperations ++= targetZipsOps(campaign, params[List[String]]("targetzips"), existingCriterion)
      }
      if(params.has("proximity")) {
        newCampaignCriterionOperations ++= proximityOps(campaign, params[ActivityParameters]("proximity"), existingCriterion)
      }
      if(params.has("adschedule")) {
        newCampaignCriterionOperations ++= adScheduleOps(campaign, params("adschedule"), existingCriterion)
      }

      adWordsAdapter.withErrorsHandled[Any]("processing Campaign Criterion", {
        adWordsAdapter.campaignCriterionService.mutate(newCampaignCriterionOperations.toArray)
      })
    }

    def updateCampaign(campaign: Campaign, params: ActivityParameters) = {

      processCampaignCriterion(campaign, params)

      if(params.has("location")) {
        setLocationExtension(campaign, params[ActivityParameters]("location"))
      }

      if(params.has("budget")) {
        val budgetName = s"${campaign.getName} Budget"
        val budget = budgetCreator.getBudget(budgetName, params[Float]("budget"))
        campaign.setBudget(budget)
      }

      if(params.has("status")) {
        campaign.setStatus(CampaignStatus.fromString(params("status")))
      }
      if(params.has("endDate")) {
        campaign.setEndDate(params[DateTime]("endDate").toString("YYYYMMdd"))
      }
      val operation = new CampaignOperation()
      operation.setOperand(campaign)
      operation.setOperator(Operator.SET)

      adWordsAdapter.withErrorsHandled[Campaign](s"updateCampaign(name='${campaign.getName}')", {
        adWordsAdapter.campaignService.mutate(Array(operation)).getValue(0)
      })

    }

    def lookupLocationsByZips(zips:Seq[String]):Array[LocationCriterion] = {

      val locations = new mutable.ArrayBuffer[LocationCriterion]()
      for(subset <- zips.sliding(25)) {

        val selector = new SelectorBuilder()
          .fields(
            "Id",
            "LocationName",
            "CanonicalName",
            "DisplayType",
            "ParentLocations",
            "Reach",
            "TargetingStatus")
          .in("LocationName", subset:_*) // Evil scala magic to splat a tuple into a Java variadic
          // Set the locale of the returned location names.
          .equals("Locale", "en")
          .build()

        val glocations = adWordsAdapter.withErrorsHandled[Array[LocationCriterion]](s"Checking zips '$subset'", {
          adWordsAdapter.locationService.get(selector)
        })

        // Make the get request.
        locations ++= ensureLocationsInUnitedStates(glocations)
      }

      locations.toArray
    }

    /**
     * This function is the result of the unfortunate fact that you can't (or at least I couldn't figure out)
     * filter by CountryCode = 'US' as you'd expect.
     * Details here: https://developers.google.com/adWordsAdapter/api/docs/appendix/selectorfields#v201409-LocationCriterionService
     * @param locations Array[LocationCriterion]
     * @return
     */
    def ensureLocationsInUnitedStates(locations:Array[LocationCriterion]) : Array[LocationCriterion] = {
      val ret = new mutable.MutableList[LocationCriterion]()
      for(loc <- locations) {
        for(ploc <- loc.getLocation.getParentLocations) {
          if(ploc.getLocationName == "United States") {
            ret += loc
          }
        }
      }
      ret.toArray
    }

    protected def _getExistingCampaignCriterion(campaign:Campaign):CampaignCriterionPage = {

      // First we query the existing criteria
      val existingSelector = new SelectorBuilder()
        .fields("Id")
        .equals("CampaignId", String.valueOf(campaign.getId))
        .build()

      adWordsAdapter.withErrorsHandled[CampaignCriterionPage]("Fetching existing Campaign criterion", {
        adWordsAdapter.campaignCriterionService.get(existingSelector)
      })
    }

    def targetZipsOps(campaign:Campaign, zips:Seq[String], existingCriterion:CampaignCriterionPage):Array[CampaignCriterionOperation] = {

      val newLocationCriteria = lookupLocationsByZips(zips)
      if(newLocationCriteria.length == 0) {
        throw new Exception(s"Target zips codes '$zips' didn't resolve to a valid list of zips!")
      }

      val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

      for(page <- existingCriterion.getEntries) {
        page.getCriterion match {
          case location: Location =>
            // Create operations to REMOVE the existing zips
            val campaignCriterion = new CampaignCriterion()
            campaignCriterion.setCampaignId(campaign.getId)
            campaignCriterion.setCriterion(location)

            val operation = new CampaignCriterionOperation()
            operation.setOperand(campaignCriterion)
            operation.setOperator(Operator.REMOVE)
            operations += operation
          case _ =>
        }
      }

      // Make the get request.
      for(loc <- newLocationCriteria) {
        val campaignCriterion = new CampaignCriterion()
        campaignCriterion.setCampaignId(campaign.getId)
        campaignCriterion.setCriterion(loc.getLocation)

        val operation = new CampaignCriterionOperation()
        operation.setOperand(campaignCriterion)
        operation.setOperator(Operator.ADD)
        operations += operation
      }

      operations.toArray
    }

    def proximityOps(campaign:Campaign, proximityParams:ActivityParameters, existingCriterion:CampaignCriterionPage):Array[CampaignCriterionOperation] = {

      val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

      for(page <- existingCriterion.getEntries) {
        page.getCriterion match {
          case proximity: Proximity =>
            // Create operations to REMOVE the existing proximities
            val campaignCriterion = new CampaignCriterion()
            campaignCriterion.setCampaignId(campaign.getId)
            campaignCriterion.setCriterion(proximity)

            val operation = new CampaignCriterionOperation()
            operation.setOperand(campaignCriterion)
            operation.setOperator(Operator.REMOVE)
            operations += operation
          case _ =>
        }
      }

      val proximity = new Proximity()
      proximity.setGeoPoint(
        new GeoPoint(
          (proximityParams[Double]("lat") * 1E6).toInt,
          (proximityParams[Double]("lon") * 1E6).toInt
      ))
      proximity.setRadiusDistanceUnits(ProximityDistanceUnits.fromString(proximityParams[String]("radiusUnits")))
      proximity.setRadiusInUnits(proximityParams[Double]("radius"))

      val campaignCriterion = new CampaignCriterion()
      campaignCriterion.setCampaignId(campaign.getId)
      campaignCriterion.setCriterion(proximity)

      val operation = new CampaignCriterionOperation()
      operation.setOperand(campaignCriterion)
      operation.setOperator(Operator.ADD)
      operations += operation

      operations.toArray
    }


    def adScheduleOps(campaign:Campaign, schedule:Seq[String], existingCriterion:CampaignCriterionPage):Array[CampaignCriterionOperation] = {

      val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

      for(page <- existingCriterion.getEntries) {
        page.getCriterion match {
          case adSchedule: AdSchedule =>
            // Create operations to REMOVE the existing schedule entries
            val campaignCriterion = new CampaignCriterion()
            campaignCriterion.setCampaignId(campaign.getId)
            campaignCriterion.setCriterion(adSchedule)

            val operation = new CampaignCriterionOperation()
            operation.setOperand(campaignCriterion)
            operation.setOperator(Operator.REMOVE)
            operations += operation
          case _ =>
        }
      }

      val shortToDayOfWeek = Map(
        "Mon" -> DayOfWeek.MONDAY,
        "Tue" -> DayOfWeek.TUESDAY,
        "Wed" -> DayOfWeek.WEDNESDAY,
        "Thu" -> DayOfWeek.THURSDAY,
        "Fri" -> DayOfWeek.FRIDAY,
        "Sat" -> DayOfWeek.SATURDAY,
        "Sun" -> DayOfWeek.SUNDAY)

      // Now create operations to add the new schedule days
      for(day <- schedule) {
        val dayOfWeek = new AdSchedule()
        dayOfWeek.setStartHour(0)
        dayOfWeek.setStartMinute(MinuteOfHour.ZERO)
        dayOfWeek.setEndHour(24)
        dayOfWeek.setEndMinute(MinuteOfHour.ZERO)
        dayOfWeek.setDayOfWeek(shortToDayOfWeek(day))

        val campaignCriterion = new CampaignCriterion()
        campaignCriterion.setCampaignId(campaign.getId)
        campaignCriterion.setCriterion(dayOfWeek)

        val operation = new CampaignCriterionOperation()
        operation.setOperand(campaignCriterion)
        operation.setOperator(Operator.ADD)
        operations += operation
      }

      operations.toArray
    }

    def setLocationExtension(campaign:Campaign, params:ActivityParameters) = {

      val address = new Address()
      address.setStreetAddress(params[String]("street address"))
      address.setCityName(params[String]("city"))
      address.setPostalCode(params[String]("postal code"))
      address.setCountryCode(params[String]("country code"))

      // First we query the existing extensions
      val existingSelector = new SelectorBuilder()
        .fields("AdExtensionId", "Address", "CompanyName", "PhoneNumber")
        .equals("CampaignId", String.valueOf(campaign.getId))
        .equals("Status", CampaignAdExtensionStatus.ENABLED.getValue)
        .equals("LocationExtensionSource", LocationExtensionSource.ADWORDS_FRONTEND.getValue)
        .build()

      val locationExtensions = new mutable.ArrayBuffer[LocationExtension]()
      var extensionExists = false
      val existing:CampaignAdExtensionPage = adWordsAdapter.campaignAdExtensionService.get(existingSelector)
      for(ext <- existing.getEntries) {
        ext.getAdExtension match {
          case le:LocationExtension =>
            locationExtensions += le
            val existingAddress = le.getAddress
            extensionExists |= (address.getStreetAddress == existingAddress.getStreetAddress &&
              address.getCityName == existingAddress.getCityName &&
              address.getPostalCode == existingAddress.getPostalCode &&
              address.getCountryCode == existingAddress.getCountryCode)
        }
      }

      if(!extensionExists) {
        _addLocationExtension(campaign, address, params)
        _removeLocationExtensions(campaign, locationExtensions.toArray)
      }

    }

    def _removeLocationExtensions(campaign:Campaign, locationExtensions:Array[LocationExtension]) = {

      val operations = new mutable.ArrayBuffer[CampaignAdExtensionOperation]()

      for(extension <- locationExtensions) {
        val campaignAdExtension = new CampaignAdExtension()
        campaignAdExtension.setCampaignId(campaign.getId)
        campaignAdExtension.setAdExtension(extension)

        val campaignAdExtensionOp = new CampaignAdExtensionOperation()
        campaignAdExtensionOp.setOperand(campaignAdExtension)
        campaignAdExtensionOp.setOperator(Operator.REMOVE)
        operations += campaignAdExtensionOp
      }

      adWordsAdapter.withErrorsHandled[Any](s"Removing ${operations.size} location extensions", {
        adWordsAdapter.campaignAdExtensionService.mutate(operations.toArray)
      })
    }

    def _addLocationExtension(campaign:Campaign, address:Address, params:ActivityParameters) = {

      val geoLocationSelector = new GeoLocationSelector()
      geoLocationSelector.setAddresses(Array(address))

      val geoLocations = adWordsAdapter.geoLocationService.get(geoLocationSelector)
      val geoLocation:Option[GeoLocation] = geoLocations.size match {
        case 1 =>
          Some(geoLocations(0))
        case _ =>
          throw new Exception("Could not resole GeoLocation for address ")
      }

      val locationExtension = new LocationExtension()
      locationExtension.setAddress(geoLocation.get.getAddress)
      locationExtension.setGeoPoint(geoLocation.get.getGeoPoint)
      locationExtension.setEncodedLocation(geoLocation.get.getEncodedLocation)
      locationExtension.setSource(LocationExtensionSource.ADWORDS_FRONTEND)

      if(params.has("company name")) { // These are optional..
        locationExtension.setCompanyName(params[String]("company name"))
      }

      if(params.has("phone number")) { // Yep.. optional
        locationExtension.setPhoneNumber(params[String]("phone number"))
      }

      val campaignAdExtension = new CampaignAdExtension()
      campaignAdExtension.setCampaignId(campaign.getId)
      campaignAdExtension.setAdExtension(locationExtension)

      val campaignAdExtensionOp = new CampaignAdExtensionOperation()
      campaignAdExtensionOp.setOperand(campaignAdExtension)
      campaignAdExtensionOp.setOperator(Operator.ADD)

      adWordsAdapter.withErrorsHandled[Any]("Adding location extension", {
        adWordsAdapter.campaignAdExtensionService.mutate(Array(campaignAdExtensionOp))
      })
    }
  }

  class CampaignCreator(awa: AdWordsAdapter)
  extends AbstractCampaignCreator
    with BudgetCreatorComponent
    with AdWordsAdapterComponent {
      val _creator = new BudgetCreator(awa)
      def adWordsAdapter = awa
      def budgetCreator = _creator
  }
}

object adwords_campaignprocessor extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsCampaignProcessor(cfg, splog)
  }
}

