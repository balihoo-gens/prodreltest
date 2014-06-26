package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.{SWFAdapter, SQSAdapter, AdWordsAdapter, RateExceededException}
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer

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

object adwords_accountlookup {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAccountLookup(
      new SWFAdapter(config)
      ,new SQSAdapter(config)
      ,new AdWordsAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
