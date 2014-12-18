package com.balihoo.fulfillment.workers.adwords

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import com.google.api.ads.adwords.axis.v201409.mcm.ManagedCustomer

abstract class AbstractAdWordsAccountLookup extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
  with AccountCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringParameter("parent", "Parent AdWords account name"),
      new StringParameter("name", "Name of the Account")
    ), new StringResultType("AdWords Account ID"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    adWordsAdapter.withErrorsHandled[ActivityResult]("Account Lookup", {
      adWordsAdapter.setClientId(accountCreator.lookupParentAccount(args))

      val aname = args[String]("name")
      accountCreator.getAccount(args) match {
        case existing:ManagedCustomer =>
          getSpecification.createResult(String.valueOf(existing.getCustomerId))
        case _ =>
          throw new FailTaskException(s"No account with name '$aname' was found!", "-")
      }
    })
  }
}



class AdWordsAccountLookup(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractAdWordsAccountLookup
  with LoggingAdwordsWorkflowAdapterImpl
  with AccountCreatorComponent {
    lazy private val _accountCreator = new AccountCreator(adWordsAdapter)
    def accountCreator = _accountCreator
}

object adwords_accountlookup extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsAccountLookup(cfg, splog)
  }
}
