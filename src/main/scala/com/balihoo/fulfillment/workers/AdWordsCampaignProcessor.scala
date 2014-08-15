package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._

import scala.collection.mutable

abstract class AbstractAdWordsCampaignProcessor extends FulfillmentWorker {
  this: AdWordsAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent
    with CampaignCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    campaignCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(params("account"))

      //val creator = new CampaignCreator with AdWordsAdapterComponent {
      //  def adWordsAdapter = AdWordsCampaignProcessor.this.adWordsAdapter
      //}

      val campaign = campaignCreator.getCampaign(params) match {
        case campaign:Campaign =>
          campaignCreator.updateCampaign(campaign, params)
        case _ =>
          campaignCreator.createCampaign(params)
      }

      completeTask(String.valueOf(campaign.getId))
    } catch {
      case rateExceeded: RateExceededException =>
        // Whoops! We've hit the rate limit! Let's sleep!
        Thread.sleep(rateExceeded.error.getRetryAfterSeconds * 1200) // 120% of the the recommended wait time
        throw rateExceeded
      case exception: Exception =>
        throw exception
      case throwable: Throwable =>
        throw new Exception(throwable.getMessage)
    }
  }
}

class AdWordsCampaignProcessor(swf: SWFAdapter, dyn: DynamoAdapter, awa: AdWordsAdapter)
  extends AbstractAdWordsCampaignProcessor
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with AdWordsAdapterComponent
  with CampaignCreatorComponent {
    //don't put this in the creator method to avoid a new one from
    //being created on every call.
    lazy val _creator = new CampaignCreator(awa)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def adWordsAdapter = awa
    def campaignCreator = _creator
}

trait BudgetCreatorComponent {
  def budgetCreator: AbstractBudgetCreator with AdWordsAdapterComponent

  abstract class AbstractBudgetCreator {
    this: AdWordsAdapterComponent =>

    def getBudget(name: String): Budget = {

      val context = s"getBudget(name='$name')"

      val selector = new SelectorBuilder()
        .fields("BudgetId")
        .equals("BudgetName", name)
        .build()

      adWordsAdapter.withErrorsHandled[Budget](context, {
        val page = adWordsAdapter.budgedService.get(selector)
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
      money.setMicroAmount(adWordsAdapter.dollarsToMicros(amount.toFloat))
      budget.setAmount(money)
      budget.setName(name)
      budget.setDeliveryMethod(BudgetBudgetDeliveryMethod.STANDARD)
      budget.setPeriod(BudgetBudgetPeriod.DAILY)
      //    budget.setIsExplicitlyShared(true)

      val operation = new BudgetOperation()
      operation.setOperand(budget)
      operation.setOperator(Operator.ADD)

      adWordsAdapter.withErrorsHandled[Budget](context, {
        adWordsAdapter.budgedService.mutate(Array(operation)).getValue(0)
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
        new ActivityParameter("account", "int", "Participant AdWords account ID"),
        new ActivityParameter("name", "string", "Name of the Campaign"),
        new ActivityParameter("channel", "SEARCH|DISPLAY|SHOPPING", "The advertising channel"),
        new ActivityParameter("budget", "float", "The monthly budget"),
        new ActivityParameter("status", "ACTIVE|PAUSED|DELETED", "Always ACTIVE on Campaign creation", false),
        new ActivityParameter("startDate", "YYYYMMDD", "Ignored on update."),
        new ActivityParameter("endDate", "YYYYMMDD", ""),
        new ActivityParameter("targetzips", "string", "Comma separated list of zip codes"),
        new ActivityParameter("adschedule", "string", "M,T,W,Th,F,S,Su")
      ))
    }

    def getCampaign(params: ActivityParameters):Campaign = {

      val name = params("name")
      val channel = params("channel")
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

    def getCampaign(id:String):Campaign = {

      val context = s"getCampaign(id='$id')"

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

      val name = params("name")
      val channel = params("channel")
      val context = s"createCampaign(name='$name', channel='$channel')"

      val budgetName = s"$name Budget"
      val budget:Budget = budgetCreator.getBudget(budgetName) match {
        case b:Budget => b
        case _ =>
          budgetCreator.createBudget(budgetName, params("budget"))
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

      val madeCampaign = adWordsAdapter.withErrorsHandled[Campaign](context, {
        adWordsAdapter.campaignService.mutate(Array(operation)).getValue(0)
      })

      setTargetZips(madeCampaign, params("targetzips"))
      setAdSchedule(madeCampaign, params("adschedule"))
      madeCampaign
    }

    def updateCampaign(campaign: Campaign, params: ActivityParameters) = {

      val context = s"updateCampaign(name='${campaign.getName}')"

      for((param, value) <- params.params) {
        param match {
          case "status" =>
            campaign.setStatus(CampaignStatus.fromString(value))
  //        case "startDate" =>
  //          campaign.setStartDate(value)
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

      adWordsAdapter.withErrorsHandled[Campaign](context, {
        adWordsAdapter.campaignService.mutate(Array(operation)).getValue(0)
      })
    }

    /**
     * This function is the result of the unfortunate fact that you can't (or at least I couldn't figure out)
     * filter by CountryCode = 'US' as you'd expect.
     * Details here: https://developers.google.com/adWordsAdapter/api/docs/appendix/selectorfields#v201402-LocationCriterionService
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

    def setTargetZips(campaign:Campaign, zipString:String) = {

      val context = s"Setting target zips for campaign ${campaign.getId}"

      val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

      // First we query the existing zips
      val existingSelector = new SelectorBuilder()
        .fields("Id")
        .equals("CampaignId", String.valueOf(campaign.getId))
        .build()

      val existingZips:CampaignCriterionPage = adWordsAdapter.campaignCriterionService.get(existingSelector)
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
      val locationCriteria = ensureLocationsInUnitedStates(adWordsAdapter.locationService.get(selector))
      for(loc <- locationCriteria) {
        val campaignCriterion = new CampaignCriterion()
        campaignCriterion.setCampaignId(campaign.getId)
        campaignCriterion.setCriterion(loc.getLocation)

        val operation = new CampaignCriterionOperation()
        operation.setOperand(campaignCriterion)
        operation.setOperator(Operator.ADD)
        operations += operation
      }

      adWordsAdapter.withErrorsHandled[Any](context, {
        adWordsAdapter.campaignCriterionService.mutate(operations.toArray)
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

      val existingSchedule:CampaignCriterionPage = adWordsAdapter.campaignCriterionService.get(selector)
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

      adWordsAdapter.withErrorsHandled[Any](context, {
        adWordsAdapter.campaignCriterionService.mutate(operations.toArray)
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


object adwords_campaignprocessor {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsCampaignProcessor(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new AdWordsAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

