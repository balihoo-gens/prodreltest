package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader
import play.api.libs.json.{JsArray, JsString, Json, JsObject}

class AdWordsWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(task: ActivityTask) = {
    adwordsAdapter.setClientId("000-000-0000") // SET AN INVALID CONTEXT TO START!!

    try {
      name match {
        case "createaccount" =>
          createAccount(task)
        case "createcampaign" =>
          createCampaign(task)
        case "createadgroup" =>
          createAdGroup(task)
        case "createimagead" =>
          createImageAd(task)
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

  def createAccount(task:ActivityTask) = {
    val input:JsObject = Json.parse(task.getInput).as[JsObject]

    adwordsAdapter.setClientId(getRequiredParameter("parent", input, task.getInput))

    val creator = new AccountCreator(adwordsAdapter)

    val name = getRequiredParameter("name", input, task.getInput)
    val existing = creator.getAccount(name)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(task.getTaskToken, String.valueOf(existing.getCustomerId))
    }

    val created = creator.createAccount(
      name,
      getRequiredParameter("currencyCode", input, task.getInput),
      getRequiredParameter("timeZone", input, task.getInput)
    )

    completeTask(task.getTaskToken, String.valueOf(created.getCustomerId))
  }

  def createCampaign(task:ActivityTask) = {
    val input:JsObject = Json.parse(task.getInput).as[JsObject]

    adwordsAdapter.setClientId(getRequiredParameter("account", input, task.getInput))

    val creator = new CampaignCreator(adwordsAdapter)

    val name = getRequiredParameter("name", input, task.getInput)
    val channel = getRequiredParameter("channel", input, task.getInput)
    val existing = creator.getCampaign(name, channel)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(task.getTaskToken, String.valueOf(existing.getId))
    }

    val created = creator.createCampaign(
      name,
      channel,
      getRequiredParameter("budget", input, task.getInput)
    )

    creator.setTargetZips(created, getRequiredParameter("targetzips", input, task.getInput))
    creator.setAdSchedule(created, getRequiredParameter("adschedule", input, task.getInput))

    completeTask(task.getTaskToken, String.valueOf(created.getId))
  }

  def createAdGroup(task:ActivityTask) = {
    val input:JsObject = Json.parse(task.getInput).as[JsObject]

    adwordsAdapter.setClientId(getRequiredParameter("account", input, task.getInput))

    val creator = new AdGroupCreator(adwordsAdapter)

    val name = getRequiredParameter("name", input, task.getInput)
    val campaignId = getRequiredParameter("campaignId", input, task.getInput)
    val existing = creator.getAdGroup(name, campaignId)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(task.getTaskToken, String.valueOf(existing.getId))
    }

    val created = creator.createAdGroup(
      name,
      campaignId
    )

    val rawtarget = getRequiredParameter("target", input, task.getInput)
    val target = Json.parse(rawtarget).as[JsObject]
    val focus = target.value("focus").as[JsString].value

    focus match {
      case "interests" =>
        val interests = target.value("interests").as[JsObject].value("interests").as[JsArray]
        creator.addUserInterests(created, for(i <- interests.value.toArray) yield i.as[String])
      case "keywords" =>
        val keywords = target.value("keywords").as[JsObject].value("keywords").as[JsString]
        creator.addKeywords(created, for(s <- keywords.value.split(",")) yield s.trim)
      case _ =>
    }

    completeTask(task.getTaskToken, String.valueOf(created.getId))
  }

  def createImageAd(task:ActivityTask) = {
    val input:JsObject = Json.parse(task.getInput).as[JsObject]

    adwordsAdapter.setClientId(getRequiredParameter("account", input, task.getInput))

    val creator = new AdCreator(adwordsAdapter)

    val name = getRequiredParameter("name", input, task.getInput)
    val adGroupId = getRequiredParameter("adGroupId", input, task.getInput)
    val existing = creator.getImageAd(name, adGroupId)

    if(existing != null) { // Look up the account first.. we don't want duplicates
      completeTask(task.getTaskToken, String.valueOf(existing.getId))
    }

    val created = creator.createImageAd(
      name,
      getRequiredParameter("url", input, task.getInput),
      getRequiredParameter("displayUrl", input, task.getInput),
      getRequiredParameter("imageUrl", input, task.getInput),
      adGroupId
    )

    completeTask(task.getTaskToken, String.valueOf(created.getId))
  }
}

object adwordsworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".adwordsworker.properties")
    val adwords = new AdWordsWorker(new SWFAdapter(config), new SQSAdapter(config), new AdWordsAdapter(config))
    println("Running adwords worker")
    adwords.work()
  }
}
