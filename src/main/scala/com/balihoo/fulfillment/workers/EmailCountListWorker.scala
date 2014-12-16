package com.balihoo.fulfillment.workers

import java.net.URI

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

  object FilterListQueryActivityParameter
    extends ObjectActivityParameter("query", "JSON representation of a SQL query", List(
      new ObjectActivityParameter("select", "select columns definition", required = true)
    ), required = true)

  override lazy val getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      List(
        FilterListQueryActivityParameter,
        new UriActivityParameter("source", "URL to a database file to use"),
        new StringActivityParameter("column", "location column name"),
        new StringsActivityParameter("locations", "location ids array", required = false)
      ),
      new ObjectActivityResult("query count (total) for each location")
    )
  }

  override def handleTask(params: ActivityParameters) = {
    withTaskHandling {

      splog.info(s"Checking parameters...")
      val source = params[URI]("source")
      val column = params[String]("column")
      val query = Json.parse(params[ActivityParameters]("query").input).as[EmailQueryDefinition]
      val locations = params[Seq[String]]("locations")
      if (source.getScheme != "s3") throw ActivitySpecificationException("Invalid source protocol")
      query.validate()
      val (sourceBucket, sourceKey) = (source.getHost, source.getPath.tail)

      splog.info(s"Downloading database file...")
      val dbMeta = workerResource(s3Adapter.getMeta(sourceBucket, sourceKey).get)
      val dbFile = workerFile(filesystemAdapter.newTempFileFromStream(dbMeta.getContentStream, sourceKey))

      splog.info(s"Connecting to database...")
      val db = workerResource(liteDbAdapter.create(dbFile.getAbsolutePath))
      val dbColumns = db.getTableColumnNames(query.getTableName)
      query.checkColumns(dbColumns)

      val results = db.executeAndGetResult(query.selectCountOnColumn(column, locations.toSet))

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
