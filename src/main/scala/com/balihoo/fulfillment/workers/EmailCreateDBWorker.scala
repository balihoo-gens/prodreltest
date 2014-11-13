package com.balihoo.fulfillment.workers

import java.io.{InputStreamReader, File}

import com.amazonaws.services.s3.model.S3Object
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json.Json

/**
 * Worker that creates a database file from a csv file, based on a dtd.
 *
 * TODO (jmelanson) create indexes at the end of insertions.
 * TODO (jmelanson) prevent sql injection the smart way.
 */
abstract class AbstractEmailCreateDBWorker extends FulfillmentWorker {

  this: LoggingWorkflowAdapter
    with S3AdapterComponent
    with CsvAdapterComponent
    with LightweightDatabaseAdapterComponent =>

  /** Supported database types. */
  sealed trait DbTypes
  case object VaryingCharactersDbType extends DbTypes
  case object CharactersDbType extends DbTypes
  case object DateDbType extends DbTypes
  case object IntegerDbType extends DbTypes

  /** Simple SQL column definition. */
  case class ColumnDefinition(name: String, `type`: String, source: String, index: Option[String])

  /** Simple SQL table definition. */
  case class TableDefinition(columns: Seq[ColumnDefinition]) {

    /** Column names by column source. */
    val source2name = columns.map(col => col.source.toLowerCase -> col.name.toLowerCase).toMap

    /** Column types by column name. */
    val name2dbType: Map[String, DbTypes] = columns.map(col => col.`type`.toLowerCase match {
        case columnType if columnType.startsWith("varchar") => col.name.toLowerCase -> VaryingCharactersDbType
        case columnType if columnType.startsWith("char") => col.name.toLowerCase -> CharactersDbType
        case columnType if columnType.startsWith("int") => col.name.toLowerCase -> IntegerDbType
        case columnType if columnType.startsWith("date") => col.name.toLowerCase -> DateDbType
    }).toMap

    /** Return a SQL statement to create table. */
    def toSql = "create table recipients (" +
      columns.map(col => col.name.toLowerCase + " " + col.`type`.toLowerCase).mkString(", ") + ")"

    splog.debug("namesBySource=" + source2name)
    splog.debug("typesByName=" + name2dbType)
    splog.debug("table definition sql=" + toSql)
  }

  /** Table definition JSON format. */
  implicit val tableColumnDefinition = Json.format[ColumnDefinition]
  implicit val tableDefinitionFormat = Json.format[TableDefinition]

  val skippedColumnName = "__skipped_column__"
  val insertBatchSize = 100000

  override lazy val getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      List(
        new ActivityParameter("source", "string", "URL that indicates where the source data is downloaded from (S3)"),
        new ActivityParameter("dbname", "string", "Name of the sqlite file that will be generated"),
        new ActivityParameter("dtd", "string", "JSON configuration document that describes the columns: SQL data type, name mappings from source to canonical, indexes, etc. (more to come)")
      ),
      new ActivityResult("string", "Name of the sqlite file that will be generated"),
      "process all rows in a csv to a sql lite database file")
  }

  /**
   * Extract, validate and return parameters for this task.
   */
  private def getParams(parameters: ActivityParameters) = {

    splog.info("Parsing parameters source, dbname and dtd")

    val maybeSource = parameters.get("source")
    val maybeDbName = parameters.get("dbname")
    // TODO (jmelanson) use json type when supported
    val maybeDtd = parameters.get("dtd")

    if (!maybeSource.isDefined || maybeSource.get.trim.isEmpty)
      throw new IllegalArgumentException("source parameter is empty")

    val sourceUri = new java.net.URI(maybeSource.get)

    if (!maybeSource.get.trim.startsWith("s3"))
      throw new IllegalArgumentException("source protocol is unsupported for now")

    if (!maybeDbName.isDefined || maybeDbName.get.trim.isEmpty)
      throw new IllegalArgumentException("dbname parameter is empty")

    if (!maybeDtd.isDefined || maybeDtd.get.trim.isEmpty)
      throw new IllegalArgumentException("dtd parameter is empty")

    val tableDefinition = Json.parse(maybeDtd.get).as[TableDefinition]
    (sourceUri.getHost, sourceUri.getPath.substring(1), maybeDbName.get, tableDefinition)
  }

  /**
   * Uploads local db to S3.
   * @return url to the resulting db s3 object.
   */
  private def s3upload(file: File, name: String) = {
    splog.info("Uploading DB file to S3")
    val targetBucket = swfAdapter.config.getString("s3bucket")
    /* left pad sub dir with slash if not present */
    val targetDir = {
      val dir = swfAdapter.config.getString("s3dir")
      if (dir.startsWith("/")) dir.substring(1) else dir
    }
    val targetKey = s"$targetDir/${System.currentTimeMillis}/$name"

    s3Adapter.putPublic(targetBucket, targetKey, file)

    s"s3://$targetBucket/$targetKey"
  }

  /**
   * Event handler for a row that contains wrong columns count.
   */
  private def handleBadRow(rowNum: Integer) = {
    if (swfAdapter.config.getOrElse("failOnBadRecord", default = true)) {
      throw new RuntimeException(s"CSV contains bad row, aborting! rowNumber=$rowNum")
    } else {
      splog.warning(s"Skipping bad record, rowNumber=$rowNum")
    }
  }

  /**
   * Writes the stream of CSV records (list of strings) into the given db.
   * A table definition is required for mapping types and column names between csv and db.
   */
  private def writeCsvStreamToDb(csvStream: Stream[List[String]], tableDefinition: TableDefinition, db: DB_TYPE) = {

    /*
      Map CSV headers to column names, using dtd.
      If CSV header not found, add it as a 'skipped' column.
     */
    val csvColumnNames = csvStream.head.map(header => tableDefinition.source2name.getOrElse(header.toLowerCase, skippedColumnName))
    if (csvColumnNames.isEmpty) throw new RuntimeException("Wrong CSV / DTD mapping: no columns found!")
    splog.debug(s"Ordered CSV columns : $csvColumnNames")

    val nonSkippedColumns = csvColumnNames.filterNot(_ == skippedColumnName)
    val sqlColumnNames = nonSkippedColumns.mkString(", ")
    splog.debug(s"Ordered SQL insert columns : $sqlColumnNames")

    /* skip header, work on rows from now on */
    val rows = csvStream.drop(1)
    if (rows.isEmpty) throw new RuntimeException("CSV stream has headers, but no rows")

    /* generates param size */
    val params = ("?" * nonSkippedColumns.size).mkString(", ")
    val dbBatch = db.batch(s"insert into recipients ($sqlColumnNames) values ($params)")

    /**
     * @return type-value tuple sequence for known column names (based on index)
     */
    def extractTypesAndValues(row: List[String]) = {
      row.view.zipWithIndex.flatMap({ case (value, index) =>
        csvColumnNames(index) match {
          case `skippedColumnName` => None /* Column is undefined in DTD */
          case columnName => Some(tableDefinition.name2dbType(columnName) -> value)
        }
      })
    }

    /**
     * Add a parameter to the prepared statement based on db type and index.
     */
    def addSqlParam(sqlParamIndex: Int, dbType: DbTypes, value: String) = dbType match {
      case VaryingCharactersDbType | CharactersDbType | DateDbType => dbBatch.param(sqlParamIndex, value)
      case IntegerDbType => dbBatch.param(sqlParamIndex, Integer.parseInt(value))
      case unhandledDbType => throw new RuntimeException(s"unhandled db type=$unhandledDbType")
    }

    /* main loop, insert all rows in db */
    var rowNumber = 0
    for (row <- rows) {
      rowNumber += 1
      if (row.size != csvColumnNames.size) {
        /* CSV row column count does not match CSV header column count */
        handleBadRow(rowNumber)
      } else {
        /* extract all types and value from row and add as prepared statement parameters */
        extractTypesAndValues(row)
          .view
          .zipWithIndex
          .foreach({ case ((dbType, value), index) => addSqlParam(index + 1, dbType, value)})

        dbBatch.add()

        /* execute batch each X records */
        if (rowNumber % insertBatchSize == 0) {
          dbBatch.execute()
          splog.info(s"Executing batch insert, count=$rowNumber")
        }
      }
    }

    dbBatch.execute() /* make sure last records are inserted */

    splog.info(s"Testing DB...")
    val recipientsCount = db.selectCount(s"select count(*) from recipients")
    splog.info(s"Done with SQL inserts, recipientsDbCount=$recipientsCount")
  }

  /**
   * @return an input stream reader (for clean up) and a scala stream based on it for
   *         processing the CSV records as they come.
   */
  private def csvStreamFromS3Content(bucket: String, key: String) = {
    splog.info(s"Streaming CSV content from S3 bucket=$bucket key=$key")
    val reader = s3Adapter.getObjectContentAsReader(bucket, key)
    val csvStream = csvAdapter.parseReaderAsStream(reader)
    if (csvStream.isEmpty) throw new RuntimeException("csv stream is empty")
    (reader, csvStream)
  }

  override def handleTask(params: ActivityParameters) = {

    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    /* extract and validate params */
    val (bucket, key, filename, tableDefinition) = getParams(params)

    val (csvReader, csvStream) = csvStreamFromS3Content(bucket, key)

    splog.info("Creating DB file")
    val db = liteDbAdapter.create(filename)

    try {

      splog.info("Creating DB schema")
      db.execute(tableDefinition.toSql)

      splog.info("Writing CSV records to DB")
      val time = System.currentTimeMillis()
      writeCsvStreamToDb(csvStream, tableDefinition, db)
      splog.info("csv to sql time=" + (System.currentTimeMillis() - time))
      db.commit()

    } finally {
      db.close()
      csvReader.close()
    }

    val s3url = s3upload(db.file, filename)

    db.destroy()

    /* return s3 url to target db file */
    splog.info(s"Task completed, target=[$s3url]")
    completeTask(s3url)

  }
}

class EmailCreateDBWorker(override val _cfg: PropertiesLoader, override val _splog: Splogger) extends AbstractEmailCreateDBWorker
  with ScalaCsvAdapterComponent
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent
  with SqlLiteLightweightDatabaseAdapterComponent {
    override val s3Adapter = new S3Adapter(_cfg)
}

object email_createdb extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker =
    new EmailCreateDBWorker(cfg, splog)
}
