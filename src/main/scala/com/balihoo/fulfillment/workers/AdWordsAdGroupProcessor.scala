package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

import scala.collection.mutable

class AdWordsAdGroupProcessor(swfAdapter: SWFAdapter,
                              sqsAdapter: SQSAdapter,
                              adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new AdGroupCreator(adwordsAdapter)

      val adGroup = creator.getAdGroup(params) match {
        case group: AdGroup =>
          creator.updateAdGroup(group, params)
        case _ =>
          creator.createAdGroup(params)
      }

      completeTask(String.valueOf(adGroup.getId))
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

class AdGroupCreator(adwords:AdWordsAdapter) {

  def getAdGroup(params:ActivityParameters): AdGroup = {

    val name = params.getRequiredParameter("name")
    val campaignId = params.getRequiredParameter("campaignId")
    val context = s"getAdGroup(name='$name', campaignId='$campaignId')"

    val selector = new SelectorBuilder()
      .fields("Id", "BiddingStrategyType")
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

    val ccreator = new CampaignCreator(adwords)
    val campaign = ccreator.getCampaign(campaignId)

    val adGroup = new AdGroup()
    adGroup.setName(name)
    adGroup.setCampaignId(campaignId.toLong)
    adGroup.setStatus(AdGroupStatus.fromString(params.getRequiredParameter("status")))

    val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
    val money = new Money()
    money.setMicroAmount(adwords.dollarsToMicros(params.getRequiredParameter("bidDollars").toFloat))

    campaign.getBiddingStrategyConfiguration.getBiddingStrategyType match {
      case BiddingStrategyType.MANUAL_CPC =>
        val bid = new CpcBid()
        bid.setBid(money)
        biddingStrategyConfiguration.setBids(Array(bid))
      case BiddingStrategyType.MANUAL_CPM =>
        val bid = new CpmBid()
        bid.setBid(money)
        biddingStrategyConfiguration.setBids(Array(bid))
      case _ =>
        throw new Exception(s"biddingStrategy ${campaign.getBiddingStrategyConfiguration.getBiddingStrategyType} is not supported!")
    }

    adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration)

    val operation = new AdGroupOperation()
    operation.setOperand(adGroup)
    operation.setOperator(Operator.ADD)

    val newGroup = adwords.withErrorsHandled[AdGroup](context, {
      adwords.adGroupService.mutate(Array(operation)).getValue(0)
    })

    addTargeting(newGroup, params.getRequiredParameter("target"))

    newGroup
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

  def addTargeting(adGroup:AdGroup, targetJson:String) = {
    val target = Json.parse(targetJson).as[JsObject]
    val focus = target.value("focus").as[JsString].value

    focus match {
      case "interests" =>
        val interests = target.value("interests").as[JsArray]
        addUserInterests(adGroup, for(i <- interests.value.toArray) yield i.as[String])
      case "keywords" =>
        val keywords = target.value("keywords").as[JsString]
        addKeywords(adGroup, for(s <- keywords.value.split(",")) yield s.trim)
      case _ =>
    }
  }

  def updateAdGroup(adGroup:AdGroup, params:ActivityParameters) = {

    val context = s"updateAdGroup(name='${adGroup.getId}', params=$params)"

    for((param, value) <- params.params) {
      param match {
        case "status" =>
          adGroup.setStatus(AdGroupStatus.fromString(value))
        case "bidDollars" =>
          val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
          val money = new Money()
          money.setMicroAmount(adwords.dollarsToMicros(value.toFloat))

          adGroup.getBiddingStrategyConfiguration.getBiddingStrategyType match {
            case BiddingStrategyType.MANUAL_CPC =>
              val bid = new CpcBid()
              bid.setBid(money)
              biddingStrategyConfiguration.setBids(Array(bid))
            case BiddingStrategyType.MANUAL_CPM =>
              val bid = new CpmBid()
              bid.setBid(money)
              biddingStrategyConfiguration.setBids(Array(bid))
            case _ =>
              throw new Exception(s"biddingStrategy ${adGroup.getBiddingStrategyConfiguration.getBiddingStrategyType} is not supported!")
          }

          adGroup.setBiddingStrategyConfiguration(biddingStrategyConfiguration)
        case "target" =>
          addTargeting(adGroup, value)
        case _ =>

      }
    }

    // TODO Update Interests and Keywords

    val operation = new AdGroupOperation()
    operation.setOperand(adGroup)
    operation.setOperator(Operator.SET)

    adwords.withErrorsHandled[AdGroup](context, {
      adwords.adGroupService.mutate(Array(operation)).getValue(0)
    })
  }
}

object adwords_adgroupprocessor {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAdGroupProcessor(
      new SWFAdapter(config)
      ,new SQSAdapter(config)
      ,new AdWordsAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

object test_adwordsAdGroupCreator {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)
    val ccreator = new CampaignCreator(adwords)
    val acreator = new AdGroupCreator(adwords)

    adwords.setValidateOnly(false)
    adwords.setClientId("100-019-2687")

    val campaignParams =
      s"""{
       "name" : "fulfillment Campaign",
        "channel" : "DISPLAY"
      }"""
    val campaign = ccreator.getCampaign(new ActivityParameters(campaignParams))

    val target =
      """{\"focus\" : \"interests\",\"interests\" : [\"Beauty & Fitness\",\"Books & Literature\"]}"""

    val adGroupParams =
      s"""{
       "name" : "CPM AdGroup",
       "status" : "ENABLED",
        "campaignId" : "${campaign.getId}",
        "bidDollars" : "6.6",
        "target" : "$target"
      }"""

    val adGroup = acreator.getAdGroup(new ActivityParameters(adGroupParams))

//    val newAdgroup = acreator.createAdGroup(new ActivityParameters(adGroupParams))
    val newAdgroup = acreator.updateAdGroup(adGroup, new ActivityParameters(adGroupParams))

    println(newAdgroup.getId)

  }
}

object test_adwordsAdGroupSetInterests {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
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

    acreator.addUserInterests(adgroup, Array("Vehicle Shows", "Livestock"))

  }
}

object test_adwordsAdGroupSetKeywords {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
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

