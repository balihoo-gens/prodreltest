package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.AdWordsUserInterests
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm._
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

import scala.collection.mutable

abstract class AdWordsAdGroupProcessor extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {
  this: AdWordsAdapterComponent =>

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new AdGroupCreator with AdWordsAdapterComponent {
        def adWordsAdapter = AdWordsAdGroupProcessor.this.adWordsAdapter
      }

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
      case throwable: Throwable =>
        throw new Exception(throwable.getMessage)
    }
  }
}

abstract class AdGroupCreator {
  this: AdWordsAdapterComponent =>

  def getAdGroup(params:ActivityParameters): AdGroup = {

    val name = params.getRequiredParameter("name")
    val campaignId = params.getRequiredParameter("campaignId")
    val context = s"getAdGroup(name='$name', campaignId='$campaignId')"

    val selector = new SelectorBuilder()
      .fields("Id", "BiddingStrategyType")
      .equals("Name", name)
      .equals("CampaignId", campaignId)
      .build()

    adWordsAdapter.withErrorsHandled[AdGroup](context, {
      val page = adWordsAdapter.adGroupService.get(selector)
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

    val ccreator = new CampaignCreator with AdWordsAdapterComponent {
      def adWordsAdapter = AdGroupCreator.this.adWordsAdapter
    }
    val campaign = ccreator.getCampaign(campaignId)

    val adGroup = new AdGroup()
    adGroup.setName(name)
    adGroup.setCampaignId(campaignId.toLong)
    adGroup.setStatus(AdGroupStatus.fromString(params.getRequiredParameter("status")))

    val biddingStrategyConfiguration = new BiddingStrategyConfiguration()
    val money = new Money()
    money.setMicroAmount(adWordsAdapter.dollarsToMicros(params.getRequiredParameter("bidDollars").toFloat))

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

    val newGroup = adWordsAdapter.withErrorsHandled[AdGroup](context, {
      adWordsAdapter.adGroupService.mutate(Array(operation)).getValue(0)
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

    adWordsAdapter.withErrorsHandled[Any](context, {
      adWordsAdapter.adGroupCriterionService.mutate(operations.toArray)
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

    adWordsAdapter.withErrorsHandled[Any](context, {
      adWordsAdapter.adGroupCriterionService.mutate(operations.toArray)
    })
  }

  def updateTargeting(adGroup:AdGroup, targetJson:String) = {

    val context = s"updateTargeting(name='${adGroup.getId}', $targetJson)"
    val operations = new mutable.ArrayBuffer[AdGroupCriterionOperation]()

    // First we query the existing criteria
    val existingSelector = new SelectorBuilder()
      .fields("Id")
      .equals("AdGroupId", String.valueOf(adGroup.getId))
      .build()

    val existing:AdGroupCriterionPage = adWordsAdapter.adGroupCriterionService.get(existingSelector)
    for(page <- existing.getEntries) {
      page.getCriterion match {
        case keyword: Keyword =>
          // Create operations to REMOVE the existing keywords
          val criterion = new BiddableAdGroupCriterion()
          criterion.setAdGroupId(adGroup.getId)
          criterion.setCriterion(keyword)

          val operation = new AdGroupCriterionOperation()
          operation.setOperand(criterion)
          operation.setOperator(Operator.REMOVE)
          operations += operation
        case interest: CriterionUserInterest =>
          // Create operations to REMOVE the existing interests
          val criterion = new BiddableAdGroupCriterion()
          criterion.setAdGroupId(adGroup.getId)
          criterion.setCriterion(interest)

          val operation = new AdGroupCriterionOperation()
          operation.setOperand(criterion)
          operation.setOperator(Operator.REMOVE)
          operations += operation
        case _ =>
      }
    }

    adWordsAdapter.withErrorsHandled[Any](context, {
      adWordsAdapter.adGroupCriterionService.mutate(operations.toArray)
    })

    addTargeting(adGroup, targetJson)
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
          money.setMicroAmount(adWordsAdapter.dollarsToMicros(value.toFloat))

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
          updateTargeting(adGroup, value)
        case _ =>

      }
    }

    val operation = new AdGroupOperation()
    operation.setOperand(adGroup)
    operation.setOperator(Operator.SET)

    adWordsAdapter.withErrorsHandled[AdGroup](context, {
      adWordsAdapter.adGroupService.mutate(Array(operation)).getValue(0)
    })
  }
}

object adwords_adgroupprocessor {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAdGroupProcessor
      with SWFAdapterComponent with DynamoAdapterComponent with AdWordsAdapterComponent {
        def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
        def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
        def adWordsAdapter = new AdWordsAdapter with PropertiesLoaderComponent { def config = cfg }
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

