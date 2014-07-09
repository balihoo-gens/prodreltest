package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment._
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer

class AdWordsAccountLookup(swfAdapter: SWFAdapter,
                           dynamoAdapter: DynamoAdapter,
                           adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

  val creator = new AccountCreator(adwordsAdapter)

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(creator.lookupParentAccount(params))

      val aname = params.getRequiredParameter("name")
      creator.getAccount(params) match {
        case existing:ManagedCustomer =>
          completeTask(String.valueOf(existing.getCustomerId))
        case _ =>
          failTask(s"No account with name '$aname' was found!", "-")
      }
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

