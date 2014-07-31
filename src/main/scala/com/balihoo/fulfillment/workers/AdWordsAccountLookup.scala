package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.config.{PropertiesLoader,PropertiesLoaderComponent}
import com.balihoo.fulfillment.adapters._
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer

abstract class AbstractAdWordsAccountLookup extends FulfillmentWorker {
  this: AdWordsAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent
    with AccountCreatorComponent =>

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(accountCreator.lookupParentAccount(params))

      val aname = params.getRequiredParameter("name")
      accountCreator.getAccount(params) match {
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

class AdWordsAccountLookup(swf: SWFAdapter, dyn: DynamoAdapter, awa: AdWordsAdapter)
  extends AbstractAdWordsAccountLookup
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with AdWordsAdapterComponent
  with AccountCreatorComponent {
    //don't put this in the accountCreator method to avoid a new one from
    //being created on every call.
    val _accountCreator = new AccountCreator(awa)
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def adWordsAdapter = awa
    def accountCreator = _accountCreator
}

object adwords_accountlookup {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAccountLookup(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new AdWordsAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

