package com.balihoo.fulfillment

import com.balihoo.fulfillment.workers.ActivityParameters

import scala.language.implicitConversions

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

  def dollarsToMicros(dollars:Float):Long = {
    (dollars * 1000000).toLong
  }

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
      case _:Throwable =>
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

  def getAccount(params:ActivityParameters):ManagedCustomer = {
    val name = params.getRequiredParameter("name")
    val context = s"getAccount(name='$name')"

    val selector = new SelectorBuilder()
      .fields("CustomerId")
      .equals("Name", name)
      .build()

    adwords.withErrorsHandled[ManagedCustomer](context, {
      val page = adwords.managedCustomerService.get(selector)
      page.getTotalNumEntries.intValue() match {
        case 0 => null
        case 1 => page.getEntries(0)
        case _ => throw new Exception(s"Account name $name is ambiguous!")
      }
    })
  }

  def createAccount(params:ActivityParameters):ManagedCustomer = {

    val name = params.getRequiredParameter("name")
    val currencyCode = params.getRequiredParameter("currencyCode")
    val timeZone = params.getRequiredParameter("timeZone")
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
      .fields("Id", "ServingStatus", "Name", "AdvertisingChannelType")
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

    adwords.withErrorsHandled[Campaign](context, {
      adwords.campaignService.mutate(Array(operation)).getValue(0)
    })
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

  def getAdGroup(params:ActivityParameters): AdGroup = {

    val name = params.getRequiredParameter("name")
    val campaignId = params.getRequiredParameter("campaignId")
    val context = s"getAdGroup(name='$name', campaignId='$campaignId')"

    val selector = new SelectorBuilder()
      .fields("Id")
      .equals("Name", name)
      .equals("CampaignId", campaignId)
      .build()

    adwords.withErrorsHandled[AdGroup](context, {
      val page = adwords.adGroupService.get(selector)
      page.getTotalNumEntries.intValue() match {
        case 0 => null
        case 1 => page.getEntries(0)
        case _ => throw new Exception(s"adGroup name $name is ambiguous in campaign $campaignId")
      }
    })
  }

  def createAdGroup(params:ActivityParameters): AdGroup = {

    val name = params.getRequiredParameter("name")
    val campaignId = params.getRequiredParameter("campaignId")
    val context = s"createAdGroup(name='$name', campaignId='$campaignId')"

    val adGroup = new AdGroup()
    adGroup.setName(name)
    adGroup.setCampaignId(campaignId.toLong)
    adGroup.setStatus(AdGroupStatus.fromString(params.getRequiredParameter("status")))

    val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
    val money = new Money()
    money.setMicroAmount(adwords.dollarsToMicros(params.getRequiredParameter("bid").toFloat))
    val bid = new CpcBid()
    bid.setBid(money)
    biddingStrategyConfiguration.setBids(Array(bid))

    adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration)

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

  def getImageAd(params: ActivityParameters): ImageAd = {

    val name = params.getRequiredParameter("name")
    val adGroupId = params.getRequiredParameter("adGroupId")
    val context = s"getImageAd(name='$name', adGroup='$adGroupId')"

    val selector = new SelectorBuilder()
      .fields("Id")
      .equals("ImageCreativeName", name)
      .equals("AdGroupId", adGroupId)
      .build()

    adwords.withErrorsHandled[ImageAd](context, {
      val page = adwords.adGroupAdService.get(selector)
      page.getTotalNumEntries.intValue() match {
        case 0 => null
        case 1 => page.getEntries(0).getAd.asInstanceOf[ImageAd]
        case _ => throw new Exception(s"imageAd name $name is ambiguous in adGroup '$adGroupId'")
      }
    })
  }

  def createImageAd(params:ActivityParameters): ImageAd = {

    val name = params.getRequiredParameter("name")
    val url = params.getRequiredParameter("url")
    val displayUrl = params.getRequiredParameter("displayUrl")
    val imageUrl = params.getRequiredParameter("imageUrl")
    val adGroupId = params.getRequiredParameter("adGroupId")

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


  def updateImageAd(ad:ImageAd, params:ActivityParameters): ImageAd = {

    val name = params.getRequiredParameter("name")
    val adGroupId = params.getRequiredParameter("adGroupId")

    val context = s"updateImageAd(name='$name', adGroupId='$adGroupId', params='$params')"

    for((param, value) <- params.params) {
      param match {
        case "url" =>
          ad.setUrl(value)
        case "displayUrl" =>
          ad.setDisplayUrl(value)
        case _ =>
      }
    }

    val aga = new AdGroupAd()
    aga.setAd(ad)
    aga.setAdGroupId(adGroupId.toLong)

    val operation = new AdGroupAdOperation()
    operation.setOperand(aga)
    operation.setOperator(Operator.SET)

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
    val accountParams =
      s"""{
       "name" : "test campaign",
        "currencyCode" : "USD",
        "timeZone" : "America/Boise"
      }"""
    val newId = creator.createAccount(new ActivityParameters(accountParams))

  }

}

object test_adwordsCampaignCreator {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwords.properties")
    val adwords = new AdWordsAdapter(config)
    val creator = new CampaignCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "test campaign",
        "channel" : "DISPLAY",
        "budget" : "11",
        "adschedule" : "M,T",
        "status" : "PAUSED",
        "startDate" : "20140625",
        "endDate" : "20140701",
        "targetzips" : "83704,83713"
      }"""

    val newCampaign = creator.createCampaign(new ActivityParameters(campaignParams))

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
    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = creator.getCampaign(new ActivityParameters(campaignParams))
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
    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = creator.getCampaign(new ActivityParameters(campaignParams))
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

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))

    val adGroupParams =
      s"""{
       "name" : "test adgroup",
       "status" : "ENABLED",
        "campaignId" : "${campaign.getId}",
        "bid" : "2.5"
      }"""
    val newAdgroup = acreator.createAdGroup(new ActivityParameters(adGroupParams))

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

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))

    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}",
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

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

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

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

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    val imageAdParams =
      s"""{
       "name" : "Another Nature",
        "adGroupId" : "${adgroup.getId}"
      }"""
    val ad = adcreator.getImageAd(new ActivityParameters(imageAdParams))

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

    val campaignParams =
      s"""{
       "name" : "fulfillment campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))
    val adgroupParams =
      s"""{
       "name" : "GROUP A",
        "campaignId" : "${campaign.getId}"
      }"""

    val adgroup = acreator.getAdGroup(new ActivityParameters(adgroupParams))

    val imageAdParams =
      s"""{
       "name" : "Another Nature",
        "adGroupId" : "${adgroup.getId}",
        "url" : "http://balihoo.com",
        "displayUrl" :    "http://balihoo.com",
        "imageUrl" : "http://lorempixel.com/300/100/nature/"
      }"""

    adcreator.createImageAd(new ActivityParameters(imageAdParams))

  }
}
