package com.balihoo.fulfillment.workers

import java.io.OutputStream
import java.net.URI

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

/**
 * Worker that execute a SQL query over a database file ot yield a set of CSV files
 * (put in s3) that contains recipients email address for bulk email delivery.
 */
abstract class AbstractEmailFilterListWorker extends FulfillmentWorker {

  this: LoggingWorkflowAdapter
    with LightweightDatabaseAdapterComponent
    with S3AdapterComponent
    with CsvAdapterComponent
    with FilesystemAdapterComponent =>

  /** Implicit Query json formatter. */
  implicit val queryDefinitionFormat = Json.format[QueryDefinition]

  val s3keySepCharPattern = "[/]".r
  val s3filenameSepCharPattern = "[\\.]".r
  val s3BucketConfig = "s3bucket"
  val s3DirConfig = "s3dir"

  def destinationS3Bucket = swfAdapter.config.getString(s3BucketConfig)
  def destinationS3Key = swfAdapter.config.getString(s3DirConfig)

  override lazy val getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      List(
        new ObjectActivityParameter("query", "JSON representation of a SQL query"),
        new StringActivityParameter("source", "URL to a database file to use"),
        new IntegerActivityParameter("pageSize", "Maximum records the produced csv files can contain")
      ),
      new StringsActivityResult("List of URLs to resulting CSV file"),
      "transform a json query to a sql query and perform that query against the specified database file")
  }

  /**
   * Extract, validate and return parameters for this task.
   */
  private def getParams(params: ActivityParameters) = {
    val maybeQuery =  params.get[JsValue]("query")
    if (maybeQuery.isEmpty) throw new IllegalArgumentException("query param is required")

    val queryDefinition = Try(maybeQuery.get.as[QueryDefinition]) match {
      case Success(td) => td
      case Failure(t) => throw new IllegalArgumentException("invalid select query object", t)
    }

    val maybeSource = params.get[String]("source")
    if (maybeSource.isEmpty || maybeSource.get.trim().isEmpty) throw new IllegalArgumentException("source param is empty")

    val source = Try(new URI(maybeSource.get)) match {
      case Success(src) => src
      case Failure(t) => throw new IllegalArgumentException("invalid source URI", t)
    }

    if (!source.getScheme.equalsIgnoreCase("s3")) throw new IllegalArgumentException("invalid source URI scheme")

    val maybePageSize =  params.get[Int]("pageSize")
    if (maybePageSize.isEmpty) throw new IllegalArgumentException("pageSize param is required")
    val pageSize = maybePageSize.get
    if (pageSize < 1) throw new IllegalArgumentException("pageSize param is invalid")

    (queryDefinition, source.getHost, source.getPath.tail, pageSize)
  }

  override def handleTask(params: ActivityParameters) = {

    val (queryDefinition, sourceBucket, sourceKey, recordsPerPage) = getParams(params)

    val dbDownload = s3Adapter.get(sourceBucket, sourceKey).map(s3Adapter.download).get
    val db = liteDbAdapter.create(dbDownload.absolutePath)

    try {

      val totalRecordsCount = db.selectCount(s"""select count(*) from "${queryDefinition.getTableName}"""")
      val queryRecordsCount = db.selectCount(queryDefinition.selectCountSql)
      val pagesCount = liteDbAdapter.calculatePageCount(queryRecordsCount, recordsPerPage)

      splog.info(s"Executing paged query... totalRecordsCount=$totalRecordsCount queryRecordsCount=$queryRecordsCount recordsPerPage=$recordsPerPage pagesCount=$pagesCount")
      val pages = db.pagedSelect(queryDefinition.selectSql, queryRecordsCount, recordsPerPage)

      /* Process all csv files one after the other */
      var pageNum = 0
      val uris = for (page <- pages) yield {
        pageNum += 1
        splog.info(s"Processing csv file #$pageNum...")

        val csvS3Key = s"$destinationS3Key/${dbDownload.meta.filename}.$pageNum.csv"
        val csvTempFile = filesystemAdapter.newTempFile(csvS3Key)
        val csvOutputStream = csvTempFile.asOutputStream
        val csvWriter = csvAdapter.newWriter(csvOutputStream)

        try {

          /* use same sql pages as csv pages for now */
          splog.info("Writing records to CSV...")
          csvWriter.writeRow(queryDefinition.fields)
          for (row <- page) {
            println(row)
            csvWriter.writeRow(row)
          }

          s3Adapter
            .upload(csvS3Key, csvTempFile.file)
            .map(_.toString)
            .get

        } finally {
          csvOutputStream.close()
          csvTempFile.delete()
        }
      }

      completeTask(JsArray(uris.toSeq.map(JsString)).toString())

    } finally {
      db.close()
      dbDownload.close()
    }
  }

}

/**
 * SQL query definition.
 *
 * The `select` field contains a set of sql criterion(s) per field.
 * Format is 1 json object per field name and this object contains
 * either a single string or an array of strings. Those strings are
 * sql where clause criterions to be combined to produce a sql query.
 * These criterions are combined with sql logical disjunctions (`or`)
 * at the field level and combined again with a sql logical
 * conjunction (`and`) at the select level.
 *
 * Utilities helps building the end where clause and select expression.
 */
case class QueryDefinition(select: JsObject, tableName: Option[String] = Some("recipients")) {

  /**
   * Placeholder in json model for field name in clauses.
   */
  val fieldNamePlaceholder = "\\$v"

  /**
   * Get table name to use for query.
   */
  val getTableName = tableName.getOrElse("recipients")

  /**
   * List of field names to be returned by select statement.
   */
  val fields = select.fields.map(_._1)

  /**
   * Map field name to a sequence of sql criterions.
   */
  val field2criterions = select.fields.flatMap { case (name, jsValue) =>
    jsValue match {
      /* Only support non empty string and array of strings for now. */
      case JsArray(elements) if elements.nonEmpty => Some(name -> elements.map(_.as[String]))
      case JsString(value) if value.trim.nonEmpty => Some(name -> Seq(value))
      case _ => None
    }
  }

  /**
   * Map field name to a an expression (combined criterions)
   */
  val field2criterionsExpression = field2criterions.map { case (name, criterions) =>
    val innerCriterions = criterions.map("(" + _.replaceAll(fieldNamePlaceholder, s""""$name"""") + ")")
    criterions.size match {
      case size if size > 1 => innerCriterions.mkString("(", " or ", ")")
      case _ => innerCriterions.mkString("")
    }
  }

  /**
   * SQL select columns expression.
   */
  val columnsExpression = fields.map("\"" + _ + "\"").mkString(", ")

  /**
   * SQL where expression.
   */
  val whereExpression = field2criterionsExpression.mkString(" and ")

  /**
   * SQL select expression to get projected query count.
   */
  val selectCountSql = s"""select count(*) from "$getTableName" where $whereExpression"""

  /**
   * SQL select expression to get query results.
   */
  val selectSql = s"""select $columnsExpression from "$getTableName" where $whereExpression order by "${fields.head}""""

  /**
   * Check generated SQL is valid.
   */
  def validate() = {
    if (whereExpression.isEmpty) throw new RuntimeException("SQL is empty")
    if (whereExpression.contains(";")) throw new RuntimeException("SQL contains reserved separator ';'")
  }
}

/**
 * Production-ready worker class.
 */
class EmailFilterListWorker(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailFilterListWorker
    with LoggingWorkflowAdapterImpl
    with SQLiteLightweightDatabaseAdapterComponent
    with S3AdapterComponent
    with ScalaCsvAdapterComponent
    with LocalFilesystemAdapterComponent {
  override val s3Adapter = new S3Adapter(_cfg, _splog)
}

/**
 * Email FilterList worker application instance.
 */
object email_filterlist extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new EmailFilterListWorker(cfg, splog)
  }
}
