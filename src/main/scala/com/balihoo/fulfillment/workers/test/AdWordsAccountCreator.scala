package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.google.api.ads.adwords.axis.utils.v201406.SelectorBuilder
import com.google.api.ads.adwords.axis.v201406.cm.Operator
import com.google.api.ads.adwords.axis.v201406.cm.Selector
import com.google.api.ads.adwords.axis.v201406.mcm.{ManagedCustomerPage, ManagedCustomerOperation, ManagedCustomer}


object adWordsGetSubaccounts {
  def main(args: Array[String]) {
    val test = new TestAdWordsGetSubAccounts(new AdWordsAdapter(PropertiesLoader(args, "adwords_accountcreator")))
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

object adWordsGetAccounts {
  def main(args: Array[String]) {
    val test = new TestAdWordsGetAccounts(new AdWordsAdapter(PropertiesLoader(args, "adwords_accountcreator")))
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

      val m:ManagedCustomer = accountCreator.getManagerAccount(new ActivityParameters(Map("parent" -> "brand-demo")))
      println(m.getCustomerId)
      println(m.getName)
      println(m.getCompanyName)
      println(m.getCurrencyCode)
      println(m.getCanManageClients)
    }
  }
}

object adWordsAccountCreator {
  def main(args: Array[String]) {
    val test = new TestAdWordsAccountCreator(new AdWordsAdapter(PropertiesLoader(args, "adwords_accountcreator")))
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

      val newId = accountCreator.createAccount(new ActivityParameters(Map(
        "name" -> "test campaign",
        "currencyCode" -> "USD",
        "timeZone" -> "America/Boise"
      )))
    }
  }
}

