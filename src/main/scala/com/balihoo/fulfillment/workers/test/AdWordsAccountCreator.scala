package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201402.SelectorBuilder
import com.google.api.ads.adwords.axis.v201402.cm.Operator
import com.google.api.ads.adwords.axis.v201402.cm.Selector
import com.google.api.ads.adwords.axis.v201402.mcm.{ManagedCustomerPage, ManagedCustomerOperation, ManagedCustomer}


object test_adWordsGetSubaccounts {
  def main(args: Array[String]) {
    val test = new TestAdWordsGetSubAccounts(new AdWordsAdapter(PropertiesLoader(args, "adWords")))
    test.run
  }

  class TestAdWordsGetSubAccounts(awa: AdWordsAdapter) extends AdWordsAdapterComponent{
    def adWordsAdapter = awa
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
    val test = new TestAdWordsGetAccounts(new AdWordsAdapter(PropertiesLoader(args, "adWords")))
    test.run
  }

  class TestAdWordsGetAccounts(awa: AdWordsAdapter)
    extends AdWordsAdapterComponent
      with AccountCreatorComponent {
    private val _creator = new AccountCreator(awa)
    def adWordsAdapter = awa
    def accountCreator = _creator

    def run() = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId(adWordsAdapter.baseAccountId) // Dogtopia

      val accountParams =
        s"""{
         "parent" : "brand-demo"
        }"""

      val m:ManagedCustomer = accountCreator.getManagerAccount(new ActivityParameters(accountParams))
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
    val test = new TestAdWordsAccountCreator(new AdWordsAdapter(PropertiesLoader(args, "adWords")))
    test.run
  }

  class TestAdWordsAccountCreator(awa: AdWordsAdapter)
    extends AdWordsAdapterComponent
      with AccountCreatorComponent {
    private val _creator = new AccountCreator(awa)
    def adWordsAdapter = awa
    def accountCreator = _creator

    def run() = {
       //    adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("981-046-8123") // Dogtopia

      val accountParams =
        s"""{
         "name" : "test campaign",
          "currencyCode" : "USD",
          "timeZone" : "America/Boise"
        }"""
      val newId = accountCreator.createAccount(new ActivityParameters(accountParams))
    }
  }
}

