package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.balihoo.fulfillment.config.PropertiesLoader
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

class AdWordsWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    adwordsAdapter.setClientId("000-000-0000") // SET AN INVALID CONTEXT TO START!!

    try {
      name match {
        case "AdWords-create-account" =>
          createAccount(params)
        case "AdWords-lookup-account" =>
          lookupAccount(params)
        case "AdWords-campaign" =>
          processCampaign(params)
        case "AdWords-adgroup" =>
          processAdGroup(params)
        case "AdWords-imagead" =>
          processImageAd(params)
        case _ =>
          throw new Exception(s"activity '$name' is NOT IMPLEMENTED")
      }
    } catch {
      case rateExceeded:RateExceededException =>
        // Whoops! We've hit the rate limit! Let's sleep!
        Thread.sleep(rateExceeded.error.getRetryAfterSeconds * 1200) // 120% of the the recommended wait time
        throw rateExceeded
      case exception:Exception =>
        throw exception
      case _:Throwable =>
        println(s"Caught a throwable!")
    }
  }

  def lookupAccount(params:ActivityParameters) = {
    adwordsAdapter.setClientId(params.getRequiredParameter("parent"))

    val creator = new AccountCreator(adwordsAdapter)

    val name = params.getRequiredParameter("name")
    val existing = creator.getAccount(name)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(String.valueOf(existing.getCustomerId))
    }

    failTask(s"No account with name '$name' was found!", "-")
  }

  def createAccount(params:ActivityParameters) = {
    adwordsAdapter.setClientId(params.getRequiredParameter("parent"))

    val creator = new AccountCreator(adwordsAdapter)

    val name = params.getRequiredParameter("name")
    val existing = creator.getAccount(name)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(String.valueOf(existing.getCustomerId))
    }

    val created = creator.createAccount(
      name,
      params.getRequiredParameter("currencyCode"),
      params.getRequiredParameter("timeZone")
    )

    completeTask(String.valueOf(created.getCustomerId))
  }

  def processCampaign(params:ActivityParameters) = {
    adwordsAdapter.setClientId(params.getRequiredParameter("account"))

    val creator = new CampaignCreator(adwordsAdapter)

    val name = params.getRequiredParameter("name")
    val channel = params.getRequiredParameter("channel")
    var campaign = creator.getCampaign(name, channel)

    if(campaign == null) { // Look up the account first.. we don't want duplicates
      campaign = creator.createCampaign(
        name,
        channel,
        params.getRequiredParameter("budget")
      )
      creator.setTargetZips(campaign, params.getRequiredParameter("targetzips"))
      creator.setAdSchedule(campaign, params.getRequiredParameter("adschedule"))

    } else {
      // An existing campaign.. update what we can..
      for((param, value) <- params.params) {
        param match {
          case "targetzips" =>
            creator.setTargetZips(campaign, value)
          case "adschedule" =>
            creator.setAdSchedule(campaign, value)
          case _ =>
            // TODO.. gripe?
        }
      }

      creator.updateCampaign(campaign, params)

    }

    completeTask(String.valueOf(campaign.getId))
  }

  def processAdGroup(params:ActivityParameters) = {
    adwordsAdapter.setClientId(params.getRequiredParameter("account"))

    val creator = new AdGroupCreator(adwordsAdapter)

    val name = params.getRequiredParameter("name")
    val campaignId = params.getRequiredParameter("campaignId")
    var adGroup = creator.getAdGroup(name, campaignId)

    var rawtarget = params.getOptionalParameter("target", "")
    if(adGroup == null) {
      adGroup = creator.createAdGroup(
        name,
        campaignId,
        params.getRequiredParameter("status")
      )
      rawtarget = params.getRequiredParameter("target")
    }

    if(rawtarget != "") {
      val target = Json.parse(rawtarget).as[JsObject]
      val focus = target.value("focus").as[JsString].value

      focus match {
        case "interests" =>
          val interests = target.value("interests").as[JsObject].value("interests").as[JsArray]
          creator.addUserInterests(adGroup, for(i <- interests.value.toArray) yield i.as[String])
        case "keywords" =>
          val keywords = target.value("keywords").as[JsObject].value("keywords").as[JsString]
          creator.addKeywords(adGroup, for(s <- keywords.value.split(",")) yield s.trim)
        case _ =>
      }
    }

    completeTask(String.valueOf(adGroup.getId))
  }

  def processImageAd(params:ActivityParameters) = {
    adwordsAdapter.setClientId(params.getRequiredParameter("account"))

    val creator = new AdCreator(adwordsAdapter)

    val name = params.getRequiredParameter("name")
    val adGroupId = params.getRequiredParameter("adGroupId")
    val existing = creator.getImageAd(name, adGroupId)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(String.valueOf(existing.getId))
    }

    val created = creator.createImageAd(
      name,
      params.getRequiredParameter("url"),
      params.getRequiredParameter("displayUrl"),
      params.getRequiredParameter("imageUrl"),
      adGroupId
    )

    completeTask(String.valueOf(created.getId))
  }
}

object adwordsworker {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val adwords = new AdWordsWorker(new SWFAdapter(config), new SQSAdapter(config), new AdWordsAdapter(config))
    println("Running adwords worker")
    adwords.work()
  }
}

