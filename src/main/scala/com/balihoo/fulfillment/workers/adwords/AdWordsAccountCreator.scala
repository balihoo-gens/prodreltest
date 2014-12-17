package com.balihoo.fulfillment.workers.adwords

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import com.balihoo.fulfillment.workers._
import com.google.api.ads.adwords.axis.utils.v201409.SelectorBuilder
import com.google.api.ads.adwords.axis.v201409.cm.{Operator, Selector}
import com.google.api.ads.adwords.axis.v201409.mcm.{ManagedCustomer, ManagedCustomerOperation}

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractAdWordsAccountCreator extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
    with AccountCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    accountCreator.getSpecification
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {
    adWordsAdapter.withErrorsHandled[ActivityResult]("Account Creator", {
      adWordsAdapter.setClientId(accountCreator.lookupParentAccount(params))

      val account = accountCreator.getAccount(params) match {
        case account:ManagedCustomer => account
        case _ =>
          accountCreator.createAccount(params)
      }
      getSpecification.createResult(String.valueOf(account.getCustomerId))
    })
  }
}

/*
 * this is a specific implementation of the default (i.e. not test) AdWordsAccountCreator
 */
class AdWordsAccountCreator(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractAdWordsAccountCreator
  with LoggingAdwordsWorkflowAdapterImpl
  with AccountCreatorComponent {
    lazy val _accountCreator = new AccountCreator(adWordsAdapter)
    def accountCreator = _accountCreator
}

trait AccountCreatorComponent {
  def accountCreator: AccountCreator with AdWordsAdapterComponent

  abstract class AbstractAccountCreator {
    this: AdWordsAdapterComponent =>

    var brandAccountCache = collection.mutable.Map[String, String]()

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new StringParameter("parent", "Parent AdWords account name"),
        new StringParameter("name", "Name of this Account"),
        new StringParameter("currencyCode", "Usually US. https://developers.google.com/adwords/api/docs/appendix/currencycodes "),
        new StringParameter("timeZone", "https://developers.google.com/adwords/api/docs/appendix/timezones")
      ), new StringResultType("AdWords Account ID"))
    }

    def getManagerAccount(params:ActivityArgs):ManagedCustomer = {
      val parent = params[String]("parent")
      val context = s"getManagerAccount(parent='$parent')"

      val selector = new SelectorBuilder()
        .fields("CustomerId", "Name", "CanManageClients")
        .equals("Name", parent)
        .equals("CanManageClients", "true")
        .build()

      _getAccount(selector, parent, context)
    }

    def getAccount(params:ActivityArgs):ManagedCustomer = {
      val name = params[String]("name")
      val context = s"getAccount(name='$name')"

      val selector = new SelectorBuilder()
        .fields("CustomerId")
        .equals("Name", name)
        .build()

      _getAccount(selector, name, context)
    }

    protected def _getAccount(selector:Selector, name:String, context:String):ManagedCustomer = {

      adWordsAdapter.withErrorsHandled[ManagedCustomer](context, {
        val page = adWordsAdapter.managedCustomerService.get(selector)
        page.getTotalNumEntries.intValue() match {
          case 0 => null
          case 1 => page.getEntries(0)
          case _ => throw new Exception(s"Account name $name is ambiguous!")
        }
      })
    }

    def createAccount(params:ActivityArgs):ManagedCustomer = {

      val name = params[String]("name")
      val currencyCode = params[String]("currencyCode")
      val timeZone = params[String]("timeZone")
      val context = s"createAccount(name='$name', currencyCode='$currencyCode', timeZone='$timeZone')"

      val customer:ManagedCustomer = new ManagedCustomer()
      customer.setName(name)
      customer.setCurrencyCode(currencyCode)
      customer.setDateTimeZone(timeZone)

      val operation:ManagedCustomerOperation = new ManagedCustomerOperation()
      operation.setOperand(customer)
      operation.setOperator(Operator.ADD)

      adWordsAdapter.withErrorsHandled[ManagedCustomer](context, {
        adWordsAdapter.managedCustomerService.mutate(Array(operation)).getValue(0)
      })
    }

    def lookupParentAccount(params:ActivityArgs):String = {
      val parentName = params[String]("parent")
      brandAccountCache.contains(parentName) match {
        case true =>
          brandAccountCache(parentName)
        case false =>
          adWordsAdapter.setClientId(adWordsAdapter.baseAccountId)
          getManagerAccount(params) match {
            case existing: ManagedCustomer =>
              brandAccountCache += (parentName -> String.valueOf(existing.getCustomerId))
              String.valueOf(existing.getCustomerId)
            case _ =>
              throw new Exception(s"No brand account with name '$parentName' was found!")
          }
      }
    }
  }

  class AccountCreator(awa: AdWordsAdapter)
    extends AbstractAccountCreator
    with AdWordsAdapterComponent {
      def adWordsAdapter = awa
  }
}

object adwords_accountcreator extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsAccountCreator(cfg, splog)
  }
}
