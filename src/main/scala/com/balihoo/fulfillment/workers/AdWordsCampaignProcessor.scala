package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

import scala.collection.mutable

class AdWordsCampaignProcessor(swfAdapter: SWFAdapter,
                               dynamoAdapter: DynamoAdapter,
                               adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new CampaignCreator(adwordsAdapter)

      val campaign = creator.getCampaign(params) match {
        case campaign:Campaign =>
          creator.updateCampaign(campaign, params)
        case _ =>
          creator.createCampaign(params)
      }

      completeTask(String.valueOf(campaign.getId))
    } catch {
      case rateExceeded: RateExceededException =>
        // Whoops! We've hit the rate limit! Let's sleep!
        Thread.sleep(rateExceeded.error.getRetryAfterSeconds * 1200) // 120% of the the recommended wait time
        throw rateExceeded
      case exception: Exception =>
        throw exception
      case _: Throwable =>
        println(s"Caught a throwable!")
    }
  }
}
class BudgetCreator(adwords:AdWordsAdapter) {

  def getBudget(name: String): Budget = {

    val context = s"getBudget(name='$name')"

    val selector = new SelectorBuilder()
      .fields("BudgetId")
      .equals("BudgetName", name)
      .build()

    adwords.withErrorsHandled[Budget](context, {
      val page = adwords.budgedService.get(selector)
      page.getTotalNumEntries.intValue() match {
        case 0 => null
        case 1 => page.getEntries(0)
        case _ => throw new Exception(s"Budget name $name is ambiguous!")
      }
    })
  }

  def createBudget(name:String, amount:String): Budget = {
    val context = s"getCampaign(name='$name', amount='$amount'"

    val budget = new Budget()
    val money = new Money()
    money.setMicroAmount(adwords.dollarsToMicros(amount.toFloat))
    budget.setAmount(money)
    budget.setName(name)
    budget.setDeliveryMethod(BudgetBudgetDeliveryMethod.STANDARD)
    budget.setPeriod(BudgetBudgetPeriod.DAILY)
    //    budget.setIsExplicitlyShared(true)

    val operation = new BudgetOperation()
    operation.setOperand(budget)
    operation.setOperator(Operator.ADD)

    adwords.withErrorsHandled[Budget](context, {
      adwords.budgedService.mutate(Array(operation)).getValue(0)
    })
  }
}

class CampaignCreator(adwords:AdWordsAdapter) {

  def getCampaign(params: ActivityParameters):Campaign = {

    val name = params.getRequiredParameter("name")
    val channel = params.getRequiredParameter("channel")
    val context = s"getCampaign(name='$name', channel='$channel'"

    val selector = new SelectorBuilder()
      .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType", "BiddingStrategyType")
      .equals("Name", name)
      .equals("AdvertisingChannelType", channel)
      .build()

    adwords.withErrorsHandled[Campaign](context, {
      val page = adwords.campaignService.get(selector)
      page.getTotalNumEntries.intValue() match {
        case 0 => null
        case 1 => page.getEntries(0)
        case _ => throw new Exception(s"Campaign name $name is ambiguous!")
      }
    })
  }

  def getCampaign(id:String):Campaign = {

    val context = s"getCampaign(id='$id')"

    val selector = new SelectorBuilder()
      .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType", "BiddingStrategyType")
      .equals("Id", id)
      .build()

    adwords.withErrorsHandled[Campaign](context, {
      val page = adwords.campaignService.get(selector)
      page.getTotalNumEntries.intValue() match {
        case 0 => null
        case 1 => page.getEntries(0)
        case _ => throw new Exception(s"Campaign id $id is ambiguous!")
      }
    })
  }

  def createCampaign(params:ActivityParameters):Campaign = {

    val name = params.getRequiredParameter("name")
    val channel = params.getRequiredParameter("channel")
    val context = s"createCampaign(name='$name', channel='$channel')"

    val budgetName = s"$name Budget"
    val budgetCreator = new BudgetCreator(adwords)
    val budget:Budget = budgetCreator.getBudget(budgetName) match {
      case b:Budget => b
      case _ =>
        budgetCreator.createBudget(budgetName, params.getRequiredParameter("budget"))
    }

    val campaignBudget = new Budget()
    campaignBudget.setBudgetId(budget.getBudgetId)

    val campaign = new Campaign()
    campaign.setName(name)
    campaign.setAdvertisingChannelType(AdvertisingChannelType.fromString(channel))

    campaign.setStatus(CampaignStatus.ACTIVE)
    campaign.setBudget(campaignBudget)

    if(params.params contains "startDate") {
      campaign.setStartDate(params.params("startDate"))
    }

    if(params.params contains "endDate") {
      campaign.setEndDate(params.params("endDate"))
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

    val keywordMatch = new KeywordMatchSetting()
    keywordMatch.setOptIn(false)

    campaign.setSettings(Array(geoTarget, keywordMatch))

    val operation = new CampaignOperation()
    operation.setOperand(campaign)
    operation.setOperator(Operator.ADD)

    val madeCampaign = adwords.withErrorsHandled[Campaign](context, {
      adwords.campaignService.mutate(Array(operation)).getValue(0)
    })

    setTargetZips(madeCampaign, params.getRequiredParameter("targetzips"))
    setAdSchedule(madeCampaign, params.getRequiredParameter("adschedule"))
    madeCampaign
  }

  def updateCampaign(campaign: Campaign, params: ActivityParameters) = {

    val context = s"updateCampaign(name='${campaign.getName}')"

    for((param, value) <- params.params) {
      param match {
        case "status" =>
          campaign.setStatus(CampaignStatus.fromString(value))
        case "startDate" =>
          campaign.setStartDate(value)
        case "endDate" =>
          campaign.setEndDate(value)
        case "targetzips" =>
          setTargetZips(campaign, value)
        case "adschedule" =>
          setAdSchedule(campaign, value)
        case _ =>
      }
    }

    val operation = new CampaignOperation()
    operation.setOperand(campaign)
    operation.setOperator(Operator.SET)

    adwords.withErrorsHandled[Campaign](context, {
      adwords.campaignService.mutate(Array(operation)).getValue(0)
    })
  }

  def setTargetZips(campaign:Campaign, zipString:String) = {

    val context = s"Setting target zips for campaign ${campaign.getId}"

    val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

    // First we query the existing zips
    val existingSelector = new SelectorBuilder()
      .fields("Id")
      .equals("CampaignId", String.valueOf(campaign.getId))
      .build()

    val existingZips:CampaignCriterionPage = adwords.campaignCriterionService.get(existingSelector)
    for(page <- existingZips.getEntries) {
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

    val selector = new SelectorBuilder()
      .fields(
        "Id",
        "LocationName",
        "CanonicalName",
        "DisplayType",
        "ParentLocations",
        "Reach",
        "TargetingStatus")
      .in("LocationName", zipString.split(","):_*) // Evil scala magic to splat a tuple into a Java variadic
      // Set the locale of the returned location names.
      .equals("Locale", "en")
      .build()

    // Make the get request.
    val locationCriteria = adwords.locationService.get(selector)
    for(loc <- locationCriteria) {
      val campaignCriterion = new CampaignCriterion()
      campaignCriterion.setCampaignId(campaign.getId)
      campaignCriterion.setCriterion(loc.getLocation)

      val operation = new CampaignCriterionOperation()
      operation.setOperand(campaignCriterion)
      operation.setOperator(Operator.ADD)
      operations += operation
    }

    adwords.withErrorsHandled[Any](context, {
      adwords.campaignCriterionService.mutate(operations.toArray)
    })
  }

  def setAdSchedule(campaign:Campaign, scheduleString:String) = {

    val context = s"setAdSchedule(campaignId='${campaign.getId}', schedule='$scheduleString')"

    val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

    // First we query the existing schedule
    val selector = new SelectorBuilder()
      .fields("Id")
      .equals("CampaignId", String.valueOf(campaign.getId))
      .build()

    val existingSchedule:CampaignCriterionPage = adwords.campaignCriterionService.get(selector)
    for(page <- existingSchedule.getEntries) {
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

    // Now create operations to add the new schedule days
    for(day <- scheduleString.split(",")) {
      val dayOfWeek = new AdSchedule()
      dayOfWeek.setStartHour(0)
      dayOfWeek.setStartMinute(MinuteOfHour.ZERO)
      dayOfWeek.setEndHour(24)
      dayOfWeek.setEndMinute(MinuteOfHour.ZERO)
      day match {
        case "M" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.MONDAY)
        case "T" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.TUESDAY)
        case "W" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.WEDNESDAY)
        case "Th" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.THURSDAY)
        case "F" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.FRIDAY)
        case "S" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.SATURDAY)
        case "Su" =>
          dayOfWeek.setDayOfWeek(DayOfWeek.SUNDAY)
        case _ =>
          // TODO Should we throw an exception!?
          dayOfWeek.setDayOfWeek(DayOfWeek.MONDAY)
      }

      val campaignCriterion = new CampaignCriterion()
      campaignCriterion.setCampaignId(campaign.getId)
      campaignCriterion.setCriterion(dayOfWeek)

      val operation = new CampaignCriterionOperation()
      operation.setOperand(campaignCriterion)
      operation.setOperator(Operator.ADD)
      operations += operation
    }

    adwords.withErrorsHandled[Any](context, {
      adwords.campaignCriterionService.mutate(operations.toArray)
    })
  }
}

object adwords_campaignprocessor {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsCampaignProcessor(
      new SWFAdapter(config)
      ,new DynamoAdapter(config)
      ,new AdWordsAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

object test_adwordsCampaignCreator {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)
    val creator = new CampaignCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

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

object test_adwordsLocationCriterion {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val zipString = "53001,53002,90210"

    val creator = new CampaignCreator(adwords)
    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = creator.getCampaign(new ActivityParameters(campaignParams))
    creator.setTargetZips(campaign, zipString)

  }
}

object test_adwordsSchedule {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val scheduleString = "T,Th"

    val creator = new CampaignCreator(adwords)
    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = creator.getCampaign(new ActivityParameters(campaignParams))
    creator.setAdSchedule(campaign, scheduleString)

  }
}

