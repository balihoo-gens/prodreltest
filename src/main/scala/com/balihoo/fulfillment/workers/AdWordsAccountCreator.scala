package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm.Operator
import com.google.api.ads.adwords.axis.v201402.cm.Selector
import com.google.api.ads.adwords.axis.v201402.mcm.{ManagedCustomerPage, ManagedCustomerOperation, ManagedCustomer}
import com.balihoo.fulfillment.util.Splogger

/*
 * trait to bundle the mixins for fulfillmentworker with an adwordsadapter
 * this is mixed in by any worker that needs adwords functionality
 */
trait LoggingAdwordsWorkflowAdapter
  extends LoggingWorkflowAdapter
  with AdWordsAdapterComponent
{}

/*
 * this is the dependency-injectable class containing all functionality
 */
abstract class AbstractAdWordsAccountCreator extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
    with AccountCreatorComponent =>

  override def getSpecification: ActivitySpecification = {
    accountCreator.getSpecification
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(accountCreator.lookupParentAccount(params))

      val account = accountCreator.getAccount(params) match {
        case account:ManagedCustomer => account
        case _ =>
          accountCreator.createAccount(params)
      }
      completeTask(String.valueOf(account.getCustomerId))
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

/*
 * this is a specific implementation of the default (i.e. not test) AdWordsAccountCreator
 */
class AdWordsAccountCreator(cfg: PropertiesLoader, splogger: Splogger)
  extends AbstractAdWordsAccountCreator
  with LoggingAdwordsWorkflowAdapter
  with AccountCreatorComponent {
    def splog = splogger

    lazy private val _swf = new SWFAdapter(cfg)
    def swfAdapter = _swf

    lazy private val _dyn = new DynamoAdapter(cfg)
    def dynamoAdapter = _dyn

    lazy private val _awa = new AdWordsAdapter(cfg)
    def adWordsAdapter = _awa

    lazy private val _accountCreator = new AccountCreator(adWordsAdapter)
    def accountCreator = _accountCreator
}

trait AccountCreatorComponent {
  def accountCreator: AccountCreator with AdWordsAdapterComponent

  abstract class AbstractAccountCreator {
    this: AdWordsAdapterComponent =>

    var brandAccountCache = collection.mutable.Map[String, String]()

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new ActivityParameter("parent", "int", "Parent AdWords account ID"),
        new ActivityParameter("name", "string", "Name of this Account"),
        new ActivityParameter("currencyCode", "string", "Usually US. https://developers.google.com/adwords/api/docs/appendix/currencycodes "),
        new ActivityParameter("timeZone", "string", "https://developers.google.com/adwords/api/docs/appendix/timezones")
      ), new ActivityResult("int", "AdWords Account ID"))
    }

    def getManagerAccount(params:ActivityParameters):ManagedCustomer = {
      val parent = params("parent")
      val context = s"getManagerAccount(parent='$parent')"

      val selector = new SelectorBuilder()
        .fields("CustomerId", "Name", "CanManageClients")
        .equals("Name", parent)
        .equals("CanManageClients", "true")
        .build()

      _getAccount(selector, parent, context)
    }

    def getAccount(params:ActivityParameters):ManagedCustomer = {
      val name = params("name")
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

    def createAccount(params:ActivityParameters):ManagedCustomer = {

      val name = params("name")
      val currencyCode = params("currencyCode")
      val timeZone = params("timeZone")
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

    def lookupParentAccount(params:ActivityParameters):String = {
      val parentName = params("parent")
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


object adwords_accountcreator {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(s"/var/log/balihoo/fulfillment/${name}.log")
    splog("INFO", s"Starting $name")
    try {
      val cfg = PropertiesLoader(args, name)
      val worker = new AdWordsAccountCreator(cfg, splog)
      worker.work()
    }
    catch {
      case t:Throwable =>
        splog("ERROR", t.getMessage)
    }
  }
}

