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
    val test = new TestAdWordsGetSubAccounts with AdWordsAdapterComponent {
      def adWordsAdapter = AdWordsAdapter(PropertiesLoader(args, "adWords"))
    }
    test.run
  }

  class TestAdWordsGetSubAccounts {
    this: AdWordsAdapterComponent =>
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
    val test = new TestAdWordsGetAccounts with AdWordsAdapterComponent {
      def adWordsAdapter = AdWordsAdapter(PropertiesLoader(args, "adWords"))
    }
    test.run
  }

  class TestAdWordsGetAccounts {
    this: AdWordsAdapterComponent =>

    def run() = {
      adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId(adWordsAdapter.baseAccountId) // Dogtopia

      val creator = new AccountCreator with AdWordsAdapterComponent {
        def adWordsAdapter = TestAdWordsGetAccounts.this.adWordsAdapter
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
    val test = new TestAdWordsAccountCreator with AdWordsAdapterComponent {
      def adWordsAdapter = AdWordsAdapter(PropertiesLoader(args, "adWords"))
    }
    test.run
  }

  class TestAdWordsAccountCreator {
    this: AdWordsAdapterComponent =>

    def run() = {
       //    adWordsAdapter.setValidateOnly(false)
      adWordsAdapter.setClientId("981-046-8123") // Dogtopia

      val creator = new AccountCreator with AdWordsAdapterComponent {
        def adWordsAdapter = TestAdWordsAccountCreator.this.adWordsAdapter
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

