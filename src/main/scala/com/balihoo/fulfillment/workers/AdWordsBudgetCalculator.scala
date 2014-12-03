package com.balihoo.fulfillment.workers

import java.io.InputStream

import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.adapters._

import com.balihoo.fulfillment.util.Splogger
import com.google.api.ads.adwords.lib.jaxb.v201409._
import org.joda.time.{Days, DateTime}

import scala.collection.JavaConversions._
import scala.collection.parallel.mutable
import scala.io.Source

abstract class AbstractAdWordsBudgetCalculator extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
  with BudgetCalculatorComponent =>

  override def getSpecification: ActivitySpecification = budgetCalculator.getSpecification

  override def handleTask(params: ActivityParameters) = {
    adWordsAdapter.withErrorsHandled[Any]("Budget Calculation", {
      adWordsAdapter.setClientId(params[String]("account"))

      completeTask(s"${budgetCalculator.computeDailyBudget(params)}")
    })
  }
}

/*
 * this is a specific implementation of the default (i.e. not test) AdWordsBudgetCalculator
 */
class AdWordsBudgetCalculator(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractAdWordsBudgetCalculator
  with LoggingAdwordsWorkflowAdapterImpl
  with BudgetCalculatorComponent {
  lazy val _budgetCalculator = new BudgetCalculator(adWordsAdapter)
  def budgetCalculator = _budgetCalculator
}

trait BudgetCalculatorComponent {
  def budgetCalculator: BudgetCalculator with AdWordsAdapterComponent

  abstract class AbstractBudgetCalculator {
    this: AdWordsAdapterComponent =>

    var brandAccountCache = collection.mutable.Map[String, String]()

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new StringActivityParameter("account", "Participant AdWords account ID"),
        new StringActivityParameter("campaignId", "AdWords Campaign ID"),
        new NumberActivityParameter("budget", "The period budget"),
        new StringActivityParameter("startDate", "YYYYMMDD", pattern=Some("[0-9]{8}")),
        new StringActivityParameter("endDate", "YYYYMMDD", pattern=Some("[0-9]{8}")),
        new StringActivityParameter("today", "YYYYMMDD", pattern=Some("[0-9]{8}")),
        new EnumsActivityParameter("adschedule", "Days of the week for spend", options=List("Mon","Tue","Wed","Thu","Fri","Sat","Sun"))
      ), new StringActivityResult("Calculated Daily budget"))
    }

    def computeDailyBudget(params:ActivityParameters):Float = {

      val startDate = params[DateTime]("startDate")
      val today = params[DateTime]("today")
      val endDate = params[DateTime]("endDate")
      val adschedule = params[List[String]]("adschedule")
      val budget = params[Float]("budget")

      val rep = _getDailyCost(params[String]("campaignId")
        ,params[String]("startDate")
        ,params[String]("today")
        ,"context lol")

      var spent = 0f
      for((date, cost) <- rep) {
        spent += adWordsAdapter.microsToDollars(cost)
      }

      var futureDays = 0
      for(day <- 0 to Days.daysBetween(today.minusDays(1), endDate.plusDays(1)).getDays) {
        if(adschedule.contains(today.plusDays(day))) {
          futureDays += 1
        }
      }

      val budgetRemaining = budget - spent
      if(budgetRemaining > 0) {
        if(futureDays > 0) {
          return budgetRemaining / futureDays
        } else {
          return budgetRemaining
        }
      }

      0.0f
    }

    def streamToString(stream:InputStream):String = {
      Source.fromInputStream(stream).mkString("")
    }

    protected def _getDailyCost(campaignId:String, startDate:String, endDate:String, context:String):Map[DateTime, Long] = {

      adWordsAdapter.withErrorsHandled[Map[DateTime, Long]](context, {
        // https://developers.google.com/adwords/api/docs/appendix/reports#campaign
        val selector = new Selector()
        selector.getFields.addAll(seqAsJavaList(List(
           "CampaignId", "Date", "Cost"
        )))

        val dateRange = new DateRange()
        dateRange.setMin(startDate)
        dateRange.setMax(endDate)
        selector.setDateRange(dateRange)

        val reportDefinition = new ReportDefinition()
        reportDefinition.setReportName("BudgetReport "+System.currentTimeMillis())
        reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.CUSTOM_DATE)
        reportDefinition.setReportType(ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT)
        reportDefinition.setDownloadFormat(DownloadFormat.TSV)
        reportDefinition.setSelector(selector)

        val report = adWordsAdapter.reportDownloader.downloadReport(reportDefinition)

        val results = collection.mutable.Map[DateTime, Long]()

        val rows = streamToString(report.getInputStream).split("""\n""")
        for(row <- rows.slice(2, rows.length - 1)) {
          val parts = row.split("""\t""")
          if(parts(0) == campaignId) {
            results(new DateTime(parts(1))) = parts(2).toLong
          }
        }

        results.toMap
      })

    }

  }

  class BudgetCalculator(awa: AdWordsAdapter)
    extends AbstractBudgetCalculator
    with AdWordsAdapterComponent {
    def adWordsAdapter = awa
  }
}

object adwords_budgetcalculator extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new AdWordsBudgetCalculator(cfg, splog)
  }
}
