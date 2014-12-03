package com.balihoo.fulfillment.workers

import java.io.InputStream

import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.adapters._

import com.balihoo.fulfillment.util.{UTCFormatter, Splogger}
import com.google.api.ads.adwords.lib.jaxb.v201409._
import org.joda.time.{Days, DateTime}

import scala.collection.JavaConversions._
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

    def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new StringActivityParameter("account", "Participant AdWords account ID"),
        new StringActivityParameter("campaignId", "AdWords Campaign ID"),
        new NumberActivityParameter("budget", "The target spend for the period from startDate to endDate"),
        new DateTimeActivityParameter("startDate", "The first date of the budget period"),
        new DateTimeActivityParameter("today", "Expected to be within startDate and endDate"),
        new DateTimeActivityParameter("endDate", "The last date of the budget period"),
        new EnumsActivityParameter("adschedule", "Days of the week for spend", options=List("Mon","Tue","Wed","Thu","Fri","Sat","Sun"))
      ), new StringActivityResult("Amount that must be spent per-remaining schedule day to spend the entire budget."))
    }

    def computeDailyBudget(params:ActivityParameters):Float = {

      val startDate = params[DateTime]("startDate")
      val today = params[DateTime]("today")
      val endDate = params[DateTime]("endDate")
      val adschedule = params[List[String]]("adschedule")
      val budget = params[Double]("budget").toFloat

      if(today.isBefore(startDate)) {
        throw new Exception(s"today (${UTCFormatter.format(today)}) is before startDate(${UTCFormatter.format(startDate)})")
      }

      if(today.isAfter(endDate)) {
        throw new Exception(s"today (${UTCFormatter.format(today)}) is after endDate(${UTCFormatter.format(endDate)})")
      }

      val rep = _getDailyCost(params[String]("campaignId")
        ,startDate.toString("YYYYMMdd")
        ,today.toString("YYYYMMdd")
        ,"Fetching Daily Spend")

      var spent = 0f
      for((date, cost) <- rep) {
        spent += adWordsAdapter.microsToDollars(cost)
      }

      var futureDays = 0
      for(day <- 0 to Days.daysBetween(today, endDate).getDays) {
        if(adschedule.contains(today.plusDays(day).dayOfWeek().getAsShortText)) {
          futureDays += 1
        }
      }

      val budgetRemaining = budget - spent
      if(budgetRemaining > 0) {
        return budgetRemaining / (if(futureDays > 0) futureDays else 1)
      }

      0.0f
    }

    def streamToString(stream:InputStream):String = {
      Source.fromInputStream(stream).mkString("")
    }

    protected def _getDailyCost(campaignId:String, startDate:String, endDate:String, context:String):Map[DateTime, Long] = {

      adWordsAdapter.withErrorsHandled[Map[DateTime, Long]](s"$context $campaignId $startDate $endDate", {
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
