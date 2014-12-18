package com.balihoo.fulfillment.workers.datastore

import java.net.URI

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import com.balihoo.fulfillment.workers._
import play.api.libs.json._

/**
 * Worker that execute a SQL query over a database file ot yield a set of CSV files
 * (put in s3) that contains recipients email address for bulk email delivery.
 */
abstract class AbstractDatabaseCsvExport extends FulfillmentWorker {

  this: LoggingWorkflowAdapter
    with LightweightDatabaseAdapterComponent
    with S3AdapterComponent
    with CsvAdapterComponent
    with FilesystemAdapterComponent =>

  def destinationS3Key = swfAdapter.config.getString("s3dir")

  override lazy val getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      List(
        new ObjectParameter("query", "JSON representation of a SQL query",
          List(new ObjectParameter("select", "select columns definition", required = true)), required = true),
        new UriParameter("source", "URL to a database file to use"),
        new IntegerParameter("pageSize", "Maximum records the produced csv files can contain")
      ),
      new StringsResultType("List of URLs to resulting CSV file"),
      "transform a json query to a sql query and perform that query against the specified database file")
  }

  override def handleTask(params: ActivityArgs) = {

    splog.info(s"Checking parameters...")
    val source = params[URI]("source")
    val query = Json.parse(params[ActivityArgs]("query").input).as[DatabaseQueryDefinition]
    val pageSize = params[Int]("pageSize")
    if (source.getScheme != "s3") throw ActivitySpecificationException("Invalid source protocol")
    val (sourceBucket, sourceKey) = (source.getHost, source.getPath.tail)

    splog.info(s"Downloading database file...")
    val dbMeta = workerResource(s3Adapter.getMeta(sourceBucket, sourceKey).get)
    val dbFile = workerFile(filesystemAdapter.newTempFileFromStream(dbMeta.getContentStream, sourceKey))

    splog.info(s"Connecting to database...")
    val db = workerResource(liteDbAdapter.create(dbFile.getAbsolutePath))
    val dbColumns = db.getAllTableColumns(query.getTableName)
    query.checkColumns(dbColumns)

    splog.info(s"Executing query count...")
    val totalRecordsCount = db.selectCount(s"""select count(*) from "${query.getTableName}"""")
    val queryRecordsCount = db.selectCount(query.selectCountSql)
    val pagesCount = liteDbAdapter.calculatePageCount(queryRecordsCount, pageSize)

    splog.info(s"Executing paged query... totalRecordsCount=$totalRecordsCount queryRecordsCount=$queryRecordsCount pageSize=$pageSize pagesCount=$pagesCount")
    val pages = db.pagedSelect(query.selectSql, queryRecordsCount, pageSize)

    /*
       Process all csv files one after the other.
       Use same sql pages as csv pages for now
     */
    val uris = for ((page, pageNum) <- pages.zipWithIndex) yield {

      splog.info(s"Processing CSV file #${pageNum + 1}...")
      val csvS3Key = s"$destinationS3Key/${dbMeta.filenameNoExtension}.${pageNum + 1}.csv.gz"
      val csvTempFile = workerFile(filesystemAdapter.newTempFile(csvS3Key))
      val csvOutputStream = workerResource(filesystemAdapter.newOutputStream(csvTempFile))
      val csvWriter = csvAdapter.newWriter(csvOutputStream)

      splog.info("Writing records to CSV...")
      csvWriter.writeRow(query.columns.toSeq)
      for (row <- page) {
        csvWriter.writeRow(row)
      }

      splog.info(s"Uploading CSV to S3... csvS3Key=$csvS3Key csvTempFile=${csvTempFile.getAbsolutePath}")
      val csvGzip = filesystemAdapter.gzip(csvTempFile)
      s3Adapter
        .upload(csvS3Key, csvGzip)
        .map(uri => JsString(uri.toString))
        .get

    }

    getSpecification.createResult(uris.toSeq)
  }


}

/**
 * Production-ready worker class.
 */
class DatabaseCsvExport(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractDatabaseCsvExport
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
object db_csv_export extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new DatabaseCsvExport(cfg, splog)
  }
}