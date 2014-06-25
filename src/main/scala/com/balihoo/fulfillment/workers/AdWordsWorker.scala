package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

class AdWordsAccountCreator(swfAdapter: SWFAdapter,
                            sqsAdapter: SQSAdapter,
                            adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("parent"))

      val creator = new AccountCreator(adwordsAdapter)
      val existing = creator.getAccount(params)

      if(existing != null) {
        // Look up the account first.. we don't want duplicates
        completeTask(String.valueOf(existing.getCustomerId))
      }

      val created = creator.createAccount(params)
      completeTask(String.valueOf(created.getCustomerId))
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

class AdWordsAccountLookup(swfAdapter: SWFAdapter,
                           sqsAdapter: SQSAdapter,
                           adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("parent"))

      val creator = new AccountCreator(adwordsAdapter)

      creator.getAccount(params) match {
        case existing:ManagedCustomer =>
          completeTask(String.valueOf(existing.getCustomerId))
        case _ =>
          failTask(s"No account with name '$name' was found!", "-")
      }
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

class AdWordsCampaignProcessor(swfAdapter: SWFAdapter,
                               sqsAdapter: SQSAdapter,
                               adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new CampaignCreator(adwordsAdapter)

      var campaign = creator.getCampaign(params)

      if(campaign == null) { // Look up the account first.. we don't want duplicates
        campaign = creator.createCampaign(params)
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

class AdWordsAdGroupProcessor(swfAdapter: SWFAdapter,
                              sqsAdapter: SQSAdapter,
                              adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new AdGroupCreator(adwordsAdapter)

      var adGroup = creator.getAdGroup(params)

      var rawtarget = params.getOptionalParameter("target", "")
      if(adGroup == null) {
        adGroup = creator.createAdGroup(params)
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

      completeTask(String.valueOf(adGroup.getId))    } catch {
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


class AdWordsImageAdProcessor(swfAdapter: SWFAdapter,
                              sqsAdapter: SQSAdapter,
                              adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("account"))

      val creator = new AdCreator(adwordsAdapter)

      val existing = creator.getImageAd(params)

      if(existing != null) {
        creator.updateImageAd(existing, params)
        completeTask(String.valueOf(existing.getId))
      }

      val created = creator.createImageAd(params)

      completeTask(String.valueOf(created.getId))
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

object adwordsworker {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val adwords = new AdWordsAccountCreator(new SWFAdapter(config), new SQSAdapter(config), new AdWordsAdapter(config))
    println("Running adwords worker")
    adwords.work()
  }
}

