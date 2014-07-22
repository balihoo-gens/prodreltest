package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm.Operator
import com.google.api.ads.adwords.axis.v201402.cm.Selector
import com.google.api.ads.adwords.axis.v201402.mcm.{ManagedCustomerPage, ManagedCustomerOperation, ManagedCustomer}

trait AdWordsAccountCreator extends FulfillmentWorker with SWFAdapterProvider with DynamoAdapterProvider {
  this: AdWordsAdapterProvider =>

  //to distinguish from the local val/type
  val ad = adWordsAdapter
  val creator = new AccountCreator with AdWordsAdapterProvider {
    lazy val adWordsAdapter = ad
  }

  override def handleTask(params: ActivityParameters) = {
    try {
      adWordsAdapter.setClientId(creator.lookupParentAccount(params))

      val account = creator.getAccount(params) match {
        case account:ManagedCustomer => account
        case _ =>
          creator.createAccount(params)
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

class AccountCreator {
  this: AdWordsAdapterProvider =>

  var brandAccountCache = collection.mutable.Map[String, String]()

  def getManagerAccount(params:ActivityParameters):ManagedCustomer = {
    val parent = params.getRequiredParameter("parent")
    val context = s"getManagerAccount(parent='$parent')"

    val selector = new SelectorBuilder()
      .fields("CustomerId", "Name", "CanManageClients")
      .equals("Name", parent)
      .equals("CanManageClients", "true")
      .build()

    _getAccount(selector, parent, context)
  }

  def getAccount(params:ActivityParameters):ManagedCustomer = {
    val name = params.getRequiredParameter("name")
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

    val name = params.getRequiredParameter("name")
    val currencyCode = params.getRequiredParameter("currencyCode")
    val timeZone = params.getRequiredParameter("timeZone")
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
    val parentName = params.getRequiredParameter("parent")
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

object adWords_accountcreator {
  def main(args: Array[String]) {
    val worker = new AdWordsAccountCreator
      with SWFAdapterProvider with DynamoAdapterProvider with AdWordsAdapterProvider {
        trait AdWordsPropertiesLoaderProvider extends PropertiesLoaderProvider {
          lazy val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
        }

        lazy val swfAdapter = new SWFAdapter with AdWordsPropertiesLoaderProvider
        lazy val dynamoAdapter = new DynamoAdapter with AdWordsPropertiesLoaderProvider
        lazy val adWordsAdapter = new AdWordsAdapter with AdWordsPropertiesLoaderProvider
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

object test_adWordsGetSubaccounts {
  def main(args: Array[String]) {
    val test = new TestAdWordsGetSubAccounts with AdWordsAdapterProvider {
      val adWordsAdapter = new AdWordsAdapter with PropertiesLoaderProvider {
        //lazy because construction init may use it
        lazy val config = PropertiesLoader(args, "adWords")
      }
    }
    test.run
  }

  class TestAdWordsGetSubAccounts {
    this: AdWordsAdapterProvider =>
    def run() = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("981-046-8123") // Dogtopia
      val ss = new SelectorBuilder().fields("Login", "CustomerId", "Name").build()

      val page:ManagedCustomerPage = adWordsAdapter.managedCustomerService.get(ss)

      for((m:ManagedCustomer) <- page.getEntries) {
        println(m.getCustomerId)
        println(m.getName)
      }
    }
  }
}

object test_adWordsGetAccounts {
  def main(args: Array[String]) {
    val test = new TestAdWordsGetAccounts with AdWordsAdapterProvider {
      val adWordsAdapter = new AdWordsAdapter with PropertiesLoaderProvider {
        //lazy because construction init may use it
        lazy val config = PropertiesLoader(args, "adWords")
      }
    }
    test.run
  }

  class TestAdWordsGetAccounts {
    this: AdWordsAdapterProvider =>

    def run() = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId(adWordsAdapter.baseAccountId) // Dogtopia

      //to distinguish from the local val/type
      val ad = adWordsAdapter
      val creator = new AccountCreator with AdWordsAdapterProvider {
        lazy val adWordsAdapter = ad
      }

      val accountParams =
        s"""{
         "parent" : "brand-demo"
        }"""

      val m:ManagedCustomer = creator.getManagerAccount(new ActivityParameters(accountParams))
      println(m.getCustomerId)
      println(m.getName)
      println(m.getCompanyName)
      println(m.getCurrencyCode)
      println(m.getCanManageClients)
    }
  }
}

object test_adWordsAccountCreator {
  def main(args: Array[String]) {
    val test = new TestAdWordsAccountCreator with AdWordsAdapterProvider {
      val adWordsAdapter = new AdWordsAdapter with PropertiesLoaderProvider {
        //lazy because construction init may use it
        lazy val config = PropertiesLoader(args, "adWords")
      }
    }
    test.run
  }

  class TestAdWordsAccountCreator {
    this: AdWordsAdapterProvider =>

    def run() = {
       //    adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("981-046-8123") // Dogtopia

      //to distinguish from the local val/type
      val ad = adWordsAdapter
      val creator = new AccountCreator with AdWordsAdapterProvider {
        lazy val adWordsAdapter = ad
      }

      val accountParams =
        s"""{
         "name" : "test campaign",
          "currencyCode" : "USD",
          "timeZone" : "America/Boise"
        }"""
      val newId = creator.createAccount(new ActivityParameters(accountParams))
    }
  }
}

