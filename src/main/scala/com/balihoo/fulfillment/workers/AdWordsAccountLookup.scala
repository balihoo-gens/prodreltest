package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment._
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer

class AdWordsAccountLookup(swfAdapter: SWFAdapter,
                           dynamoAdapter: DynamoAdapter,
                           adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

  val creator = new AccountCreator(adwordsAdapter)

  var brandAccountCache = collection.mutable.Map[String, String]()

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(lookupParentAccount(params.getRequiredParameter("parent")))

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

  def lookupParentAccount(brandKey:String):String = {
    val params = new ActivityParameters(s"""{ "name" : "$brandKey" }""")

    brandAccountCache.contains(brandKey) match {
      case true =>
        brandAccountCache(brandKey)
      case false =>
        adwordsAdapter.setClientId(adwordsAdapter.baseAccountId)
        creator.getAccount(params) match {
          case existing:ManagedCustomer =>
            brandAccountCache += (brandKey -> String.valueOf(existing.getCustomerId))
            String.valueOf(existing.getCustomerId)
          case _ =>
            throw new Exception(s"No brand account with name '$brandKey' was found!")
        }
    }
  }


}

object adwords_accountlookup {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAccountLookup(
      new SWFAdapter(config)
      ,new DynamoAdapter(config)
      ,new AdWordsAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

