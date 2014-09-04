package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.config.{PropertiesLoader,PropertiesLoaderComponent}
import com.balihoo.fulfillment.adapters._
import com.google.api.ads.adwords.axis.v201402.mcm.ManagedCustomer

import com.balihoo.fulfillment.util.Splogger

abstract class AbstractAdWordsAccountLookup extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
  with AccountCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("parent", "int", "Brand AdWords account ID"),
      new ActivityParameter("name", "string", "Name of the Account")
    ), new ActivityResult("int", "AdWords Account ID"))
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(accountCreator.lookupParentAccount(params))

      val aname = params("name")
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



class AdWordsAccountLookup(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractAdWordsAccountLookup
  with LoggingAdwordsWorkflowAdapterImpl
  with AccountCreatorComponent {
    lazy private val _accountCreator = new AccountCreator(adWordsAdapter)
    def accountCreator = _accountCreator
}

object adwords_accountlookup {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(Splogger.mkFFName(name))
    splog("INFO", s"Started $name")
    try {
      val cfg = PropertiesLoader(args, name)
      val worker = new AdWordsAccountLookup(cfg, splog)
      worker.work()
    }
    catch {
      case t:Throwable =>
        splog("ERROR", t.getMessage)
    }
    splog("INFO", s"Terminated $name")
  }
}

