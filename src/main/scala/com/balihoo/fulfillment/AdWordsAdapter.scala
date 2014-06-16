package com.balihoo.fulfillment

import scala.language.implicitConversions
import scala.collection.convert.wrapAsScala._
import scala.collection.convert.wrapAsJava._

import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.lib.client.AdWordsSession
import com.google.api.client.auth.oauth2.Credential
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api
import com.google.api.ads.common.lib.auth.OfflineCredentials
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import com.google.api.ads.adwords.axis.factory.AdWordsServices
import com.google.api.ads.adwords.axis.v201402.cm._
import com.google.api.ads.adwords.axis.v201402.mcm._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import scala.collection.mutable

class AdWordsAdapter(loader: PropertiesLoader) {
  val config = loader

  private val configuration:Configuration = new BaseConfiguration()
  configuration.addProperty("api.adwords.refreshToken", loader.getString("refreshToken"))
  configuration.addProperty("api.adwords.clientId", loader.getString("clientId"))
  configuration.addProperty("api.adwords.clientSecret", loader.getString("clientSecret"))

  // Generate a refreshable OAuth2 credential similar to a ClientLogin token
  // and can be used in place of a service account.
  private val oAuth2Credential: Credential = new OfflineCredentials.Builder()
      .forApi(Api.ADWORDS)
      .from(configuration)
      .build()
      .generateCredential()

  // Construct an AdWordsSession.
  private val session:AdWordsSession = new AdWordsSession.Builder()
    .withOAuth2Credential(oAuth2Credential)
    .withDeveloperToken(loader.getString("developerToken"))
    .withUserAgent("Balihoo_Fulfillment")
    .enableReportMoneyInMicros()
    .build()

  session.setValidateOnly(true)

  private val services = new AdWordsServices

  val campaignService:CampaignServiceInterface = services.get(session, classOf[CampaignServiceInterface])
  val campaignCriterionService:CampaignCriterionServiceInterface = services.get(session, classOf[CampaignCriterionServiceInterface])
  val adGroupService:AdGroupServiceInterface = services.get(session, classOf[AdGroupServiceInterface])
  val managedCustomerService:ManagedCustomerServiceInterface = services.get(session, classOf[ManagedCustomerServiceInterface])
  val budgedService:BudgetServiceInterface = services.get(session, classOf[BudgetServiceInterface])
  val locationService:LocationCriterionServiceInterface = services.get(session, classOf[LocationCriterionServiceInterface])

  def setClientId(id:String) = {
    session.setClientCustomerId(id)
  }

  def setValidateOnly(tf:Boolean) = {
    session.setValidateOnly(tf)
  }

}

class RateExceededException(e:RateExceededError) extends Exception {
  val error = e
}

class AccountCreator(adwords:AdWordsAdapter) {

  def getAccount(name:String):ManagedCustomer = {
    val selector = new SelectorBuilder()
      .fields("CustomerId")
      .equals("Name", name)
      .build()

    val customers:ManagedCustomerPage = adwords.managedCustomerService.get(selector)

    if(customers.getTotalNumEntries == 1) {
      customers.getEntries(0)
    } else if(customers.getTotalNumEntries > 1) {
      throw new Exception(s"Error looking up account! Name '$name' is ambiguous!")
    } else {
      null
    }
  }

  def createAccount(name:String, currencyCode:String, timeZone:String):ManagedCustomer = {

    val context = s"createAccount(name='$name', currencyCode='$currencyCode', timeZone='$timeZone')"

    val customer:ManagedCustomer = new ManagedCustomer()
    customer.setName(name)
    customer.setCurrencyCode(currencyCode)
    customer.setDateTimeZone(timeZone)

    val operation:ManagedCustomerOperation = new ManagedCustomerOperation()
    operation.setOperand(customer)
    operation.setOperator(Operator.ADD)

    try {
      val result: ManagedCustomerReturnValue = adwords.managedCustomerService.mutate(Array(operation))
      result.getValue(0)
    } catch {
      case exception:ApiException =>
        for((error) <- exception.getErrors) {
          error match {
            case rateExceeded:RateExceededError =>
              throw new RateExceededException(rateExceeded)
            case apiError:ApiError =>
              throw new Exception(apiError.getErrorString+"("+apiError.getTrigger+") path:"
                +apiError.getFieldPath+" where:"+context)
            case _ =>
              throw new Exception(error.getErrorString + " " + context)
          }
        }
        null
      case exception:Exception =>
        throw exception
      case _:Throwable =>
        throw new Exception("unhandled exception! "+ context)
    }
  }
}

class BudgetCreator(adwords:AdWordsAdapter) {

  def getBudget(name: String): Budget = {

    val selector = new SelectorBuilder()
      .fields("BudgetId")
      .equals("BudgetName", name)
      .build()

    val budgets: BudgetPage = adwords.budgedService.get(selector)

    if(budgets.getTotalNumEntries == 1) {
      budgets.getEntries(0)
    } else if(budgets.getTotalNumEntries > 1) {
      throw new Exception(s"Error looking up budget! Name '$name' is ambiguous!")
    } else {
      null
    }
  }

  def createBudget(name: String, amount: String): Budget = {

    val context = s"createBudget(name='$name', amount='$amount')"
    val budget = new Budget()
    val money = new Money()
    money.setMicroAmount((1000000 * amount.toFloat).toLong)
    budget.setAmount(money)
    budget.setName(name)
    budget.setDeliveryMethod(BudgetBudgetDeliveryMethod.STANDARD)
    budget.setPeriod(BudgetBudgetPeriod.DAILY)
//    budget.setIsExplicitlyShared(true)

    val budgetOperation = new BudgetOperation()
    budgetOperation.setOperand(budget)
    budgetOperation.setOperator(Operator.ADD)

    try {
      val result = adwords.budgedService.mutate(Array(budgetOperation))
      if(result == null) {
        throw new Exception("Failed to create Budget! " + context)
      }

      result.getValue(0)
    } catch {
      case exception: ApiException =>
        for((error) <- exception.getErrors) {
          error match {
            case rateExceeded: RateExceededError =>
              throw new RateExceededException(rateExceeded)
            case apiError: ApiError =>
              throw new Exception(apiError.getErrorString + "(" + apiError.getTrigger + ") path:"
                + apiError.getFieldPath + " where:" + context)
            case _ =>
              throw new Exception(error.getErrorString + " " + context)
          }
        }
        null
      case exception: Exception =>
        throw exception
      case _: Throwable =>
        throw new Exception("unhandled exception! " + context)
    }

  }
}

class CampaignCreator(adwords:AdWordsAdapter) {

  def getCampaign(name:String, channel:String):Campaign = {

    val selector = new SelectorBuilder()
      .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType")
      .equals("Name", name)
      .equals("AdvertisingChannelType", channel)
      .build()

    val campaigns:CampaignPage = adwords.campaignService.get(selector)

    if(campaigns.getTotalNumEntries == 1) {
      campaigns.getEntries(0)
    } else if(campaigns.getTotalNumEntries > 1) {
      throw new Exception(s"Error looking up campaign! Name '$name' is ambiguous!")
    } else {
      null
    }
  }

  def createCampaign(name:String
                    ,channel:String
                    ,budgetId:String
                      ):Campaign = {

    val context = s"createCampaign(name='$name', channel='$channel')"

    val campaignBudget = new Budget()
    campaignBudget.setBudgetId(budgetId.toLong)

    val campaign = new Campaign()
    campaign.setName(name)
    campaign.setStatus(CampaignStatus.PAUSED)
    campaign.setBudget(campaignBudget)
//    campaign.setStartDate("20140101")
//    campaign.setEndDate("20150101")
    campaign.setAdvertisingChannelType(AdvertisingChannelType.fromString(channel))

    val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
    biddingStrategyConfiguration.setBiddingStrategyType(BiddingStrategyType.MANUAL_CPC)

    // You can optionally provide a bidding scheme in place of the type.
    val cpcBiddingScheme = new ManualCpcBiddingScheme()
    cpcBiddingScheme.setEnhancedCpcEnabled(false)
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

    try {
      val result = adwords.campaignService.mutate(Array(operation))

      result.getValue(0)
    } catch {
      case exception:ApiException =>
        for((error) <- exception.getErrors) {
          error match {
            case rateExceeded:RateExceededError =>
              throw new RateExceededException(rateExceeded)
            case apiError:ApiError =>
              throw new Exception(apiError.getErrorString+"("+apiError.getTrigger+") path:"
                +apiError.getFieldPath+" where:"+context)
            case _ =>
              throw new Exception(error.getErrorString + " " + context)
          }
        }
        null
      case exception:Exception =>
        throw exception
      case _:Throwable =>
        throw new Exception("unhandled exception! "+ context)
    }
  }

  def setTargetZips(campaign:Campaign, zipString:String) = {

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

    val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

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

    adwords.campaignCriterionService.mutate(operations.toArray)
  }

  def setAdSchedule(campaign:Campaign, scheduleString:String) = {

    val operations = new mutable.ArrayBuffer[CampaignCriterionOperation]()

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

    adwords.campaignCriterionService.mutate(operations.toArray)
  }
}

class AdGroupCreator(adwords:AdWordsAdapter) {

  def getAdGroup(name: String, campaignId: String): AdGroup = {

    val selector = new SelectorBuilder()
      .fields("Id")
      .equals("Name", name)
      .equals("CampaignId", campaignId)
      .build()

    val adGroups: AdGroupPage = adwords.adGroupService.get(selector)

    if(adGroups.getTotalNumEntries == 1) {
      adGroups.getEntries(0)
    } else if(adGroups.getTotalNumEntries > 1) {
      throw new Exception(s"Error looking up adGroup! Name '$name' is ambiguous!")
    } else {
      null
    }
  }

  def createAdGroup(name: String, campaignId: String): AdGroup = {

    val context = s"createAdGroup(name='$name', campaignId='$campaignId')"

    val adGroup = new AdGroup()
    adGroup.setName(name)
    adGroup.setCampaignId(campaignId.toLong)
    adGroup.setStatus(AdGroupStatus.PAUSED)

    val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
    biddingStrategyConfiguration.setBiddingStrategyType(BiddingStrategyType.MANUAL_CPC)

    // You can optionally provide a bidding scheme in place of the type.
    val cpcBiddingScheme = new ManualCpcBiddingScheme()
    cpcBiddingScheme.setEnhancedCpcEnabled(true)
    biddingStrategyConfiguration.setBiddingScheme(cpcBiddingScheme)

    adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration)

    val operation = new AdGroupOperation()
    operation.setOperand(adGroup)
    operation.setOperator(Operator.ADD)

    try {
      val result = adwords.adGroupService.mutate(Array(operation))

      result.getValue(0)
    } catch {
      case exception: ApiException =>
        for((error) <- exception.getErrors) {
          error match {
            case rateExceeded: RateExceededError =>
              throw new RateExceededException(rateExceeded)
            case apiError: ApiError =>
              throw new Exception(apiError.getErrorString + "(" + apiError.getTrigger + ") path:"
                + apiError.getFieldPath + " where:" + context)
            case _ =>
              throw new Exception(error.getErrorString + " " + context)
          }
        }
        null
      case exception: Exception =>
        throw exception
      case _: Throwable =>
        throw new Exception("unhandled exception! " + context)
    }
  }
}

object adwordsAdapterTest {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)

    adwords.setClientId("981-046-8123") // Dogtopia
    val ss = new SelectorBuilder().fields("Login", "CustomerId", "Name").build()

    val page:ManagedCustomerPage = adwords.managedCustomerService.get(ss)

    for((m:ManagedCustomer) <- page.getEntries) {
      println(m.getCustomerId)
      println(m.getName)
    }

  }
}

object adwordsAccountCreatorTest {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)

//    adwords.setValidateOnly(false)
    adwords.setClientId("981-046-8123") // Dogtopia
    val creator = new AccountCreator(adwords)
    val newId = creator.createAccount("fulfillment test", "USD", "America/Boise")

  }

}

object adwordsCampaignCreatorTest {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val creator = new CampaignCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")
    val newCampaign = creator.createCampaign("fulfillment Campaign", "DISPLAY", "100")

    println(newCampaign.getId)

  }

}

object adwordsLocationCriterionTest {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val zipString = "83704,83713,90210"

    val creator = new CampaignCreator(adwords)
    val campaign = creator.getCampaign("fulfillment Campaign", "DISPLAY")
    creator.setTargetZips(campaign, zipString)

  }
}

object adwordsScheduleTest {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val scheduleString = "M,W,F,S"

    val creator = new CampaignCreator(adwords)
    val campaign = creator.getCampaign("fulfillment Campaign", "DISPLAY")
    creator.setAdSchedule(campaign, scheduleString)

  }
}
