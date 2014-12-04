package com.balihoo.fulfillment.workers

import java.io.ByteArrayInputStream

import com.google.api.ads.adwords.lib.utils.v201409.ReportDownloader
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse
import org.joda.time.DateTime
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._

import org.junit.runner._

import scala.language.implicitConversions

import com.balihoo.fulfillment.adapters._

/**
 * Example on how to mock up all the layers of the cake pattern
 */
@RunWith(classOf[JUnitRunner])
class TestAdWordsBudgetCalculator extends Specification with Mockito
{
  /**
   * Everything is mocked here, except the BudgetCalculator
   *  a new BudgetCalculator is instantiated here on every call
   *  to 'accountCreator'
   */
  class AdWordsBudgetCalculatorTest
    extends AbstractAdWordsBudgetCalculator
    with LoggingWorkflowAdapterTestImpl
    with LoggingAdwordsWorkflowAdapter
    with BudgetCalculatorComponent {

    /**
     * Mock objects for the LoggingAdwordsWorkflowAdapter mixins
     */
    override def adWordsAdapter = mock[AdWordsAdapter]

    val mockReportDownloader = mock[ReportDownloader]
    mockReportDownloader.downloadReport(any) returns
      new ReportDownloadResponse(200, "Good Response", new ByteArrayInputStream(
        """
          |Some Report Blah!
          |Campaign Day Cost
          |55 2014-12-01 440000
          |66 2014-12-01 550000
          |55 2014-12-02 440000
          |66 2014-12-02 880000
          |55 2014-12-03 770000
          |55 2014-12-04 750000
          |55 2014-12-05 440000
          |55 2014-12-06 880000
          |55 2014-12-07 220000
          |Totals blah blah
        """.stripMargin.getBytes
      ))

    adWordsAdapter.reportDownloader returns mockReportDownloader


    /**
     * instantiate a REAL Account creator
     */
    def budgetCalculator = new BudgetCalculator(adWordsAdapter)
  }

  /**
   * The actual test, using all the Mock objects
   */
  "AdWordsBudgetCalculator" should {
    "get schedule dates correctly" in {
      //creates an actual budget calculator with mock adapters
      val calculator = new AdWordsBudgetCalculatorTest
      val dates = calculator.budgetCalculator.getScheduleDatesForPeriod(List("Wed", "Thu", "Sat"), new DateTime("2014-01-01T12:12:12Z"), new DateTime("2014-01-17T12:12:12Z"))

      dates mustEqual Vector(
        new DateTime("2014-01-01T12:12:12Z"), // Wed
        new DateTime("2014-01-02T12:12:12Z"), // Thu
        new DateTime("2014-01-04T12:12:12Z"), // Sat
        new DateTime("2014-01-08T12:12:12Z"), // Wed
        new DateTime("2014-01-09T12:12:12Z"), // Thu
        new DateTime("2014-01-11T12:12:12Z"), // Sat
        new DateTime("2014-01-15T12:12:12Z"), // Wed
        new DateTime("2014-01-16T12:12:12Z")  // Thu
      )
    }
  }
}
