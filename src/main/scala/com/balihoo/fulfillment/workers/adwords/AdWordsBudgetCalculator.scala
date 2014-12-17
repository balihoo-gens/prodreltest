package com.balihoo.fulfillment.workers.adwords

import java.io.InputStream

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.{Splogger, UTCFormatter}
import com.google.api.ads.adwords.lib.jaxb.v201409._
import org.joda.time.{DateTime, Days}
import play.api.libs.json.{JsObject, Json}

import scala.collection.JavaConversions._
import scala.io.Source

abstract class AbstractAdWordsBudgetCalculator extends FulfillmentWorker {
  this: LoggingAdwordsWorkflowAdapter
  with BudgetCalculatorComponent =>

  override def getSpecification: ActivitySpecification = budgetCalculator.getSpecification

  override def handleTask(params: ActivityArgs):ActivityResult = {
    adWordsAdapter.withErrorsHandled[ActivityResult]("Budget Calculation", {
      adWordsAdapter.setClientId(params[String]("account"))

      getSpecification.createResult(Json.stringify(budgetCalculator.computeDailyBudget(params)))
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
        new StringParameter("account", "Participant AdWords account ID"),
        new StringParameter("campaignId", "AdWords Campaign ID"),
        new NumberParameter("budget", "The target spend for the period from startDate to endDate"),
        new DateTimeParameter("startDate", "The first date of the budget period"),
        new DateTimeParameter("today", "Expected to be within startDate and endDate"),
        new DateTimeParameter("endDate", "The last date of the budget period"),
        new EnumsParameter("adschedule", "Days of the week for spend", options=List("Mon","Tue","Wed","Thu","Fri","Sat","Sun"))
      ), new ObjectResultType("Results of budget calcuations", Map(
        "futureScheduleDays" -> new ArrayResultType("List of future dates in the schedule", new DateTimeResultType("Schedule date")),
        "futureScheduleDaysCount" -> new IntegerResultType("Count of futureScheduleDays"),
        "budgetSpent" -> new NumberResultType("The budget spent between startDate and today"),
        "budgetRemaining" -> new NumberResultType("The budget - budgetSpent"),
        "dailyBudget" -> new NumberResultType("budgetRemaining / futureScheduleDaysCount")
        )),
        "https://docs.google.com/a/balihoo.com/presentation/d/13ZZaIxekgcpFY5G4gTeSCu49V2liMyP8-0NwEyFr1q8/edit?usp=sharing"
      )
    }

    def computeDailyBudget(params:ActivityArgs):JsObject = {

      val campaignId = params[String]("campaignId")
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

      // We count up all the days between today and endDate that are in the schedule
      val futureScheduleDays = getScheduleDatesForPeriod(adschedule, today, endDate)
      val futureDateCount = futureScheduleDays.size

      val spent = adWordsAdapter.microsToDollars(getCampaignSpendForPeriod(campaignId, startDate, today))
      val budgetRemaining = budget - spent
      val dailyBudget = budgetRemaining > 0 match {
        case true =>
          budgetRemaining / (if(futureDateCount > 0) futureDateCount else 1)
        case false =>
          0.0f
      }

      Json.obj(
        "futureScheduleDays" -> (for(date <- futureScheduleDays) yield UTCFormatter.format(date)),
        "futureScheduleDaysCount" -> futureDateCount,
        "budgetSpent" -> spent,
        "budgetRemaining" -> budgetRemaining,
        "dailyBudget" -> dailyBudget
      )

    }

    def streamToString(stream:InputStream):String = {
      Source.fromInputStream(stream).mkString("")
    }

    /**
     *
     * @param weekdays List[String] Days are expected to be Joda weekday short names.. "Mon","Tue","Wed","Thu","Fri","Sat","Sun"
     * @param startDate DateTime
     * @param endDate Datetime
     * @return
     */
    def getScheduleDatesForPeriod(weekdays:List[String], startDate:DateTime, endDate:DateTime):Seq[DateTime] = {
      val days = for(day <- 0 to Days.daysBetween(startDate, endDate).getDays) yield startDate.plusDays(day)
      days.filter(d => weekdays.contains(d.dayOfWeek().getAsShortText))
    }

    /**
     *
     * @param campaignId String AdWords Campaign ID
     * @param startDate DateTime
     * @param endDate DateTime
     * @return the amount of microdollars spent during the time period
     */
    def getCampaignSpendForPeriod(campaignId:String, startDate:DateTime, endDate:DateTime):Long = {
      val spendReport = _getDailyCostSafe(campaignId, startDate, endDate)
      spendReport.foldLeft[Long](0){ (z, l) => z + l._2 }
    }


    def getDailyCost(campaignId:String, startDate:DateTime, endDate:DateTime) :Map[DateTime, Long] = {
      // https://developers.google.com/adwords/api/docs/appendix/reports#campaign
      val selector = new Selector()
      selector.getFields.addAll(seqAsJavaList(List(
        "CampaignId", "Date", "Cost"
      )))

      val dateRange = new DateRange()
      dateRange.setMin(startDate.toString("YYYYMMdd")) // AdWords wants this date format.
      dateRange.setMax(endDate.toString("YYYYMMdd"))
      selector.setDateRange(dateRange)

      val reportDefinition = new ReportDefinition()
      reportDefinition.setReportName(s"BudgetReport $campaignId "+System.currentTimeMillis())
      reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.CUSTOM_DATE)
      reportDefinition.setReportType(ReportDefinitionReportType.CAMPAIGN_PERFORMANCE_REPORT)
      reportDefinition.setDownloadFormat(DownloadFormat.TSV)
      reportDefinition.setSelector(selector)

      val report = adWordsAdapter.reportDownloader.downloadReport(reportDefinition)

      // The report is a TSV formatted like this:
      // Campaign Day Cost
      // 34563456 2014-12-01 440000
      // 34563456 2014-12-02 240000
      // 67897856 2014-12-03 560000
      // 34563456 2014-12-03 780000
      // 34563456 2014-12-04 518000

      val results = collection.mutable.Map[DateTime, Long]()

      val rows = streamToString(report.getInputStream).split("""\n""")
      for(row <- rows.slice(2, rows.length - 1)) { // Slice to skip the header and the totals
      val parts = row.split("""\t""")
        if(parts(0) == campaignId) { // Not all rows in the report are for the campaign we're interested in.
          results(new DateTime(parts(1))) = parts(2).toLong // Mapping when to how much
        }
      }

      results.toMap

    }

    /**
     *
     * @param campaignId String AdWords Campaign ID
     * @param startDate DateTime
     * @param endDate DateTime
     * @return
     */
    protected def _getDailyCostSafe(campaignId:String, startDate:DateTime, endDate:DateTime):Map[DateTime, Long] = {

      adWordsAdapter.withErrorsHandled[Map[DateTime, Long]](s"Fetching Campaign Performance Report $campaignId $startDate $endDate", getDailyCost(campaignId, startDate, endDate))
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
