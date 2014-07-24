package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.config.{PropertiesLoader,PropertiesLoaderComponent}
import com.balihoo.fulfillment.adapters._
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer

abstract class AdWordsAccountLookup extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {
  this: AdWordsAdapterComponent =>

  val creator = new AccountCreator with AdWordsAdapterComponent {
    def adWordsAdapter = AdWordsAccountLookup.this.adWordsAdapter
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(creator.lookupParentAccount(params))

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
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAccountLookup
      with SWFAdapterComponent with DynamoAdapterComponent with AdWordsAdapterComponent {
        def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
        def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
        def adWordsAdapter = new AdWordsAdapter with PropertiesLoaderComponent { def config = cfg }
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

