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
  val adGroupCriterionService:AdGroupCriterionServiceInterface = services.get(session, classOf[AdGroupCriterionServiceInterface])
  val adGroupAdService:AdGroupAdServiceInterface = services.get(session, classOf[AdGroupAdServiceInterface])
  val managedCustomerService:ManagedCustomerServiceInterface = services.get(session, classOf[ManagedCustomerServiceInterface])
  val budgedService:BudgetServiceInterface = services.get(session, classOf[BudgetServiceInterface])
  val locationService:LocationCriterionServiceInterface = services.get(session, classOf[LocationCriterionServiceInterface])
  val mediaService:MediaServiceInterface = services.get(session, classOf[MediaServiceInterface])

  def setClientId(id:String) = {
    session.setClientCustomerId(id)
  }

  def setValidateOnly(tf:Boolean = true) = {
    session.setValidateOnly(tf)
  }

  def setPartialFailure(tf:Boolean = true) = {
    // TODO Partial failure could be really beneficial here.
    // It might allow for all of the legal keywords in a set to work
    // and then we'd get an error about just the ones that failed, instead
    // of an all-or-nothing style of updating.
    session.setPartialFailure(tf)
  }

  def withErrorsHandled[T](context:String, code: => T):T = {
    try {
      code
    } catch {
      case apiException: ApiException =>
        val errors = new mutable.MutableList[String]()
        for((error) <- apiException.getErrors) {
          error match {
            case rateExceeded: RateExceededError =>
              throw new RateExceededException(rateExceeded)
            case apiError: ApiError =>
              errors += (apiError.getErrorString + "(" + apiError.getTrigger + ") path:"
                + apiError.getFieldPath + " where:" + context)
            case _ =>
              errors += error.getErrorString + " " + context

          }
        }
        throw new Exception(s"${errors.length} Errors!: " + errors.mkString("\n"))
      case e:Exception =>
        throw e
      case _ =>
        throw new Exception("Unhandled case")
    }
  }


  def addOrSet(operatorId:Long): Operator = {
    if(Option(operatorId).isEmpty) Operator.ADD else Operator.SET
  }
}

class RateExceededException(e:RateExceededError) extends Exception {
  val error = e
}

class AccountCreator(adwords:AdWordsAdapter) {

  def getAccount(name:String):ManagedCustomer = {
    val context = s"getAccount(name='$name')"

    val selector = new SelectorBuilder()
      .fields("CustomerId")
      .equals("Name", name)
      .build()

    adwords.withErrorsHandled[ManagedCustomer](context, {
      adwords.managedCustomerService.get(selector).getEntries(0)
    })
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

    adwords.withErrorsHandled[ManagedCustomer](context, {
      adwords.managedCustomerService.mutate(Array(operation)).getValue(0)
    })
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
      adwords.budgedService.get(selector).getEntries(0)
    })
  }

  def createBudget(name:String, amount:String): Budget = {
    val context = s"getCampaign(name='$name', amount='$amount'"

    val budget = new Budget()
    val money = new Money()
    money.setMicroAmount((1000000 * amount.toFloat).toLong)
    budget.setAmount(money)
    budget.setName(name)
    budget.setDeliveryMethod(BudgetBudgetDeliveryMethod.STANDARD)
    budget.setPeriod(BudgetBudgetPeriod.DAILY)
    //    budget.setIsExplicitlyShared(true)

    val operation = new BudgetOperation()
    operation.setOperand(budget)
    operation.setOperator(adwords.addOrSet(operation.getOperand.getBudgetId))

    adwords.withErrorsHandled[Budget](context, {
      adwords.budgedService.mutate(Array(operation)).getValue(0)
    })
  }
}

class CampaignCreator(adwords:AdWordsAdapter) {

  def getCampaign(name:String, channel:String):Campaign = {

    val context = s"getCampaign(name='$name', channel='$channel'"

    val selector = new SelectorBuilder()
      .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType")
      .equals("Name", name)
      .equals("AdvertisingChannelType", channel)
      .build()

    adwords.withErrorsHandled[Campaign](context, {
      adwords.campaignService.get(selector).getEntries(0)
    })
  }

  def createCampaign(name:String
                    ,channel:String
                    ,budgetDollars:String
                      ):Campaign = {

    val context = s"createCampaign(name='$name', channel='$channel')"

    val budgetName = s"$name Budget"
    val budgetCreator = new BudgetCreator(adwords)
    val budget:Budget = budgetCreator.getBudget(budgetName) match {
      case b:Budget => b
      case _ =>
        budgetCreator.createBudget(budgetName, budgetDollars)
    }

    val campaignBudget = new Budget()
    campaignBudget.setBudgetId(budget.getBudgetId)

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

    adwords.withErrorsHandled[Campaign](context, {
      adwords.campaignService.mutate(Array(operation)).getValue(0)
    })
  }

  def setTargetZips(campaign:Campaign, zipString:String) = {

    val context = s"Setting target zips for campaign ${campaign.getId}"

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

    adwords.withErrorsHandled[Any](context, {
      adwords.campaignCriterionService.mutate(operations.toArray)
    })
  }

  def setAdSchedule(campaign:Campaign, scheduleString:String) = {

    val context = s"setAdSchedule(campaignId='${campaign.getId}', schedule='$scheduleString')"
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

    adwords.withErrorsHandled[Any](context, {
      adwords.campaignCriterionService.mutate(operations.toArray)
    })
  }
}

class AdGroupCreator(adwords:AdWordsAdapter) {

  def getAdGroup(name: String, campaignId: String): AdGroup = {

    val context = s"getAdGroup(name='$name', campaignId='$campaignId')"

    val selector = new SelectorBuilder()
      .fields("Id")
      .equals("Name", name)
      .equals("CampaignId", campaignId)
      .build()

    adwords.withErrorsHandled[AdGroup](context, {
      adwords.adGroupService.get(selector).getEntries(0)
    })
  }

  def createAdGroup(name: String, campaignId: String): AdGroup = {

    val context = s"createAdGroup(name='$name', campaignId='$campaignId')"

    val adGroup = new AdGroup()
    adGroup.setName(name)
    adGroup.setCampaignId(campaignId.toLong)
    adGroup.setStatus(AdGroupStatus.PAUSED)

    val operation = new AdGroupOperation()
    operation.setOperand(adGroup)
    operation.setOperator(Operator.ADD)

    adwords.withErrorsHandled[AdGroup](context, {
      adwords.adGroupService.mutate(Array(operation)).getValue(0)
    })
  }

  def addUserInterests(adGroup:AdGroup, interests:Array[String]) = {

    val context = s"addUserInterests(name='${adGroup.getId}', $interests)"

    val operations = new mutable.ArrayBuffer[AdGroupCriterionOperation]()

    for(i <- interests) {
      val interest = new CriterionUserInterest()
      interest.setUserInterestId(AdWordsUserInterests.getInterestId(i))

      val criterion = new BiddableAdGroupCriterion()
      criterion.setAdGroupId(adGroup.getId)
      criterion.setCriterion(interest)

      val operation = new AdGroupCriterionOperation()
      operation.setOperand(criterion)
      operation.setOperator(Operator.ADD)

      operations += operation
    }

    adwords.withErrorsHandled[Any](context, {
      adwords.adGroupCriterionService.mutate(operations.toArray)
    })
  }

  def addKeywords(adGroup:AdGroup, keywords:Array[String]) = {

    val context = s"addKeywords(name='${adGroup.getId}', $keywords)"

    val operations = new mutable.ArrayBuffer[AdGroupCriterionOperation]()

    for(kw <- keywords) {
      val keyword = new Keyword()
      keyword.setText(kw)
      keyword.setMatchType(KeywordMatchType.BROAD) // TODO.. is this right?

      val criterion = new BiddableAdGroupCriterion()
      criterion.setAdGroupId(adGroup.getId)
      criterion.setCriterion(keyword)

      val operation = new AdGroupCriterionOperation()
      operation.setOperand(criterion)
      operation.setOperator(Operator.ADD)

      operations += operation
    }

    adwords.withErrorsHandled[Any](context, {
      adwords.adGroupCriterionService.mutate(operations.toArray)
    })
  }

}

class AdCreator(adwords:AdWordsAdapter) {


  def getImageAd(name: String, adGroupId:String): ImageAd = {

    val context = s"getImageAd(name='$name', adGroup='$adGroupId')"

    val selector = new SelectorBuilder()
      .fields("Id")
      .equals("ImageCreativeName", name)
      .equals("AdGroupId", adGroupId)
      .build()

    adwords.withErrorsHandled[ImageAd](context, {
      adwords.adGroupAdService.get(selector).getEntries(0).getAd.asInstanceOf[ImageAd]
    })
  }

  def createImageAd(name: String, url:String, displayUrl:String, imageUrl: String, adGroupId:String): ImageAd = {

    val context = s"createImageAd(name='$name', url='$url', displayUrl='$displayUrl', imageUrl='$imageUrl', adGroupId='$adGroupId')"

    val image = new Image()
    image.setData(
      com.google.api.ads.common.lib.utils.Media.getMediaDataFromUrl(imageUrl))
    image.setType(MediaMediaType.IMAGE)

    val ad = new ImageAd()
    ad.setImage(image)
    ad.setName(name)
    ad.setDisplayUrl(displayUrl)
    ad.setUrl(url)

    val aga = new AdGroupAd()
    aga.setAd(ad)
    aga.setAdGroupId(adGroupId.toLong)

    val operation = new AdGroupAdOperation()
    operation.setOperand(aga)
    operation.setOperator(Operator.ADD)

    adwords.withErrorsHandled[ImageAd](context, {
      adwords.adGroupAdService.mutate(Array(operation)).getValue(0).getAd.asInstanceOf[ImageAd]
    })
  }

}

// these are one-off tests against the adwords API

object test_adwordsGetSubaccounts {
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

object test_adwordsAccountCreator {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)

//    adwords.setValidateOnly(false)
    adwords.setClientId("981-046-8123") // Dogtopia
    val creator = new AccountCreator(adwords)
    val newId = creator.createAccount("fulfillment test", "USD", "America/Boise")

  }

}

object test_adwordsCampaignCreator {
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

object test_adwordsLocationCriterion {
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

object test_adwordsSchedule {
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

object test_adwordsAdGroupCreator {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaign = ccreator.getCampaign("fulfillment Campaign", "DISPLAY")

    val newAdgroup = acreator.createAdGroup("GROUP A", String.valueOf(campaign.getId))

    println(newAdgroup.getId)

  }
}

object test_adwordsAdGroupSetInterests {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaign = ccreator.getCampaign("fulfillment Campaign", "DISPLAY")
    val adgroup = acreator.getAdGroup("GROUP A", String.valueOf(campaign.getId))

    acreator.addUserInterests(adgroup, Array("Vehicle Shows", "Livestock"))

  }
}

object test_adwordsAdGroupSetKeywords {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaign = ccreator.getCampaign("fulfillment Campaign", "DISPLAY")
    val adgroup = acreator.getAdGroup("GROUP A", String.valueOf(campaign.getId))

    acreator.addKeywords(adgroup, Array("tuna", "dressage", "aluminum"))

  }
}

object test_adwordsGetAdGroupImageAd {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)
    val adcreator = new AdCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaign = ccreator.getCampaign("fulfillment Campaign", "DISPLAY")
    val adgroup = acreator.getAdGroup("GROUP A", String.valueOf(campaign.getId))

    val ad = adcreator.getImageAd("Nature", String.valueOf(adgroup.getId))

    println(ad.toString)
  }
}

object test_adwordsAdGroupImageAd {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)
    val adcreator = new AdCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaign = ccreator.getCampaign("fulfillment Campaign", "DISPLAY")
    val adgroup = acreator.getAdGroup("GROUP A", String.valueOf(campaign.getId))

    adcreator.createImageAd("Nature", "http://balihoo.com", "http://balihoo.com", "http://lorempixel.com/300/100/nature/", String.valueOf(adgroup.getId))

  }
}
