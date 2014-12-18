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

  object FilterListQueryActivityParameter
    extends ObjectParameter("query", "JSON representation of a SQL query", List(
      new ObjectParameter("select", "select columns definition", required = true)
    ), required = false)

  override def getSpecification = {
    new ActivitySpecification(
      List(
        FilterListQueryActivityParameter,
        new UriParameter("source", "URL to a database file to use"),
        new StringParameter("column", "location column name", minLength = Some(1)),
        new StringsParameter("locations", "location ids array", required = false)
      ),
      new ArrayResultType("list of counts per locations",
        new ObjectResultType("location result", Map(
          "location" -> new StringResultType("location id"),
          "count" -> new IntegerResultType("query count"))))
    )
  }

  override def handleTask(params: ActivityArgs) = {

    splog.info(s"Checking parameters...")
    val source = params[URI]("source")
    if (source.getScheme != "s3") throw ActivitySpecificationException("source parameter protocol is unsupported")
    val column = params[String]("column")
    val locations = params.get[Seq[String]]("locations").getOrElse(Seq.empty)
    if (locations.isEmpty) {
      getSpecification.createResult(Json.arr())
    } else {

      splog.info(s"Parsing query...")
      val queryDef =
        params
          .get[ActivityArgs]("query")
          .fold(new EmailQueryDefinition(Json.obj())) { query =>
            Json.parse(query.input).as[EmailQueryDefinition]
          }

      splog.info(s"Downloading database file...")
      val (sourceBucket, sourceKey) = (source.getHost, source.getPath.tail)
      val dbMeta = workerResource(s3Adapter.getMeta(sourceBucket, sourceKey).get)
      val dbFile = workerFile(filesystemAdapter.newTempFileFromStream(dbMeta.getContentStream, sourceKey))

      splog.info(s"Connecting to database...")
      val db = workerResource(liteDbAdapter.create(dbFile.getAbsolutePath))
      val dbColumns = db.getAllTableColumns(queryDef.getTableName)
      queryDef.checkColumns(dbColumns)

      splog.info(s"Querying database...")
      val selectQuery = queryDef.selectCountOnColumn(column, locations.toSet)
      val results = db.executeAndGetResult(selectQuery).flatMap {
        case first +: second +: tail =>
          val discriminant = first.asInstanceOf[String]
          val count = second.asInstanceOf[Int]
          if (count > 0) Some(Json.obj(column -> discriminant, "count" -> count)) else None
        case row => throw new IllegalStateException(s"row data count different than expected, got $row")
      }

      splog.info(s"Writing results...")
      getSpecification.createResult(JsArray(results))
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
