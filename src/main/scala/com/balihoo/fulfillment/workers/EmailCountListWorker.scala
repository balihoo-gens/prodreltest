package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._

/**
 * Worker that execute a SQL query over a database file ot yield a set of CSV files
 * (put in s3) that contains recipients email address for bulk email delivery.
 */
abstract class AbstractEmailCountListWorker extends FulfillmentWorker {

  this: LoggingWorkflowAdapter
    with LightweightDatabaseAdapterComponent
    with S3AdapterComponent
    with FilesystemAdapterComponent =>

  implicit val queryDefinitionFormat = Json.format[EmailQueryDefinition]

  val s3keySepCharPattern = "[/]".r
  val s3filenameSepCharPattern = "[\\.]".r

  object FilterListQueryActivityParameter
    extends ObjectActivityParameter("query", "JSON representation of a SQL query", List(
      new ObjectActivityParameter("select", "select columns definition", required = true)
    ), required = true)

  override lazy val getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      List(
        new StringActivityParameter("source", "URL to a database file to use"),
        new ArrayActivityParameter("queries", "Queries to use for counting",
          new ObjectActivityParameter("", "Query definition"))
      ),
      new ObjectActivityResult("an object containing a \"results\" property which returns an array of object that contains query columns and value and a count property.")
    )
  }

  override def handleTask(params: ActivityParameters) = {
    withTaskHandling {
      ""
    }
  }

}

/**
 * Production-ready worker class.
 */
class EmailCountListWorker(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailCountListWorker
    with LoggingWorkflowAdapterImpl
    with SQLiteLightweightDatabaseAdapterComponent
    with S3AdapterComponent
    with ScalaCsvAdapterComponent
    with FilesystemAdapterComponent {
  override val s3Adapter = new S3Adapter(_cfg, _splog)
}

/**
 * Email FilterList worker application instance.
 */
object email_countlist extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new EmailCountListWorker(cfg, splog)
  }
}
