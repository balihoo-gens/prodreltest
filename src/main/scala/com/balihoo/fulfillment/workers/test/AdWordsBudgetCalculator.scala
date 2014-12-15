package com.balihoo.fulfillment.workers.test

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import play.api.libs.json.Json

abstract class BudgetCalculateTest(cfg: PropertiesLoader)
  extends AdWordsAdapterComponent
  with CampaignCreatorComponent
  with BudgetCalculatorComponent {
  private val _awa = new AdWordsAdapter(cfg)
  def adWordsAdapter = _awa
  private val _cc = new CampaignCreator(adWordsAdapter)
  private val _bc = new BudgetCalculator(adWordsAdapter)
  def campaignCreator = _cc
  def budgetCalculator = _bc

  def run: Unit
}


object adWordsCalculateBudget {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, "adwords_budgetcalculator")
    val test = new TestCalculateBudget(cfg)
    test.run
  }

  class TestCalculateBudget(cfg: PropertiesLoader) extends BudgetCalculateTest(cfg) {
    def run = {
      adWordsAdapter.setValidateOnly(false)
//      adWordsAdapter.setClientId("100-019-2687") //  Balihoo > Balihoo Dogtopia | fulfillment test ( Client ID: 100-019-2687 )
      adWordsAdapter.setClientId("282-338-1220")

      val campaignParams = Map(
//        "name" -> "fulfillment Campaign",
//        "channel" -> "DISPLAY"
      "name" -> "CT-13:AFFID-410851",
      "channel" -> "DISPLAY"
      )
      val campaign = campaignCreator.getCampaign(new ActivityArgs(campaignParams))


      val budgetCalcParams = Json.obj(
        "account" -> "282-338-1220",
        "campaignId" -> campaign.getId.toString,
        "startDate" -> "2014-11-01T12:12:12Z",
        "today" -> "2014-12-29T12:12:12Z",
        "endDate" -> "2014-12-29T12:12:12Z",
        "budget" -> 100,
        "adschedule" -> List("Mon", "Tue", "Fri")
      )

      val budget = budgetCalculator.computeDailyBudget(budgetCalculator.getSpecification.getArgs(budgetCalcParams))

      println(budget)
    }
  }
}
