package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm.Operator
import com.google.api.ads.adwords.axis.v201402.mcm.{ManagedCustomerPage, ManagedCustomerOperation, ManagedCustomer}

class AdWordsAccountCreator(swfAdapter: SWFAdapter,
                            sqsAdapter: SQSAdapter,
                            adwordsAdapter: AdWordsAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    try {
      adwordsAdapter.setClientId(params.getRequiredParameter("parent"))

      val creator = new AccountCreator(adwordsAdapter)
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
      case _: Throwable =>
        println(s"Caught a throwable!")
    }
  }
}

class AccountCreator(adwords:AdWordsAdapter) {

  def getAccount(params:ActivityParameters):ManagedCustomer = {
    val name = params.getRequiredParameter("name")
    val context = s"getAccount(name='$name')"

    val selector = new SelectorBuilder()
      .fields("CustomerId")
      .equals("Name", name)
      .build()

    adwords.withErrorsHandled[ManagedCustomer](context, {
      val page = adwords.managedCustomerService.get(selector)
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

    adwords.withErrorsHandled[ManagedCustomer](context, {
      adwords.managedCustomerService.mutate(Array(operation)).getValue(0)
    })
  }
}

object adwords_accountcreator {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new AdWordsAccountCreator(
      new SWFAdapter(config)
      ,new SQSAdapter(config)
      ,new AdWordsAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

object test_adwordsGetSubaccounts {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)

    adwords.setValidateOnly(false)
    adwords.setClientId("981-046-8123") // Dogtopia
    val ss = new SelectorBuilder().fields("Login", "CustomerId", "Name").build()

    val page:ManagedCustomerPage = adwords.managedCustomerService.get(ss)

    for((m:ManagedCustomer) <- page.getEntries) {
      println(m.getCustomerId)
      println(m.getName)
    }

  }
}

object test_adwordsAccountCreator {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "adwords")
    val adwords = new AdWordsAdapter(config)

    //    adwords.setValidateOnly(false)
    adwords.setClientId("981-046-8123") // Dogtopia
    val creator = new AccountCreator(adwords)
    val accountParams =
      s"""{
       "name" : "test campaign",
        "currencyCode" : "USD",
        "timeZone" : "America/Boise"
      }"""
    val newId = creator.createAccount(new ActivityParameters(accountParams))

  }

}

