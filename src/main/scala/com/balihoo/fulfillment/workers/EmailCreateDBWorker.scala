package com.balihoo.fulfillment.workers

import java.io.File

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
  val insertBatchSize = 1000

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

  private def sanitizeDbValue(value: String) = {

    // TODO (jmelanson) figure out what to do for sql injection, this line is too dumb (snap csv has users with ; in email)
    // if (value.contains(";")) throw new RuntimeException("no semi-colon allowed in values (prevent sql injection)")

    value.replaceAll("\\'", "''")
  }

  private def writeCsvStreamToDb(csvStream: Stream[List[String]], tableDefinition: TableDefinition, db: DB_TYPE) = {

    /*
      Map CSV headers to column names, using dtd.
      If CSV header not found, add it as a 'skipped' column.
     */
    val csvColumnNames = csvStream.head.map(header => tableDefinition.source2name.getOrElse(header.toLowerCase, skippedColumnName))
    if (csvColumnNames.isEmpty) throw new RuntimeException("Wrong CSV / DTD mapping: no columns found!")
    splog.debug(s"Ordered CSV columns : $csvColumnNames")

    val insertColumns = csvColumnNames.filterNot(_ == skippedColumnName).mkString(", ")
    splog.debug(s"Ordered CSV insert columns : $insertColumns")

    val rows = csvStream.drop(1)
    if (rows.isEmpty) throw new RuntimeException("CSV stream has headers, but no rows")

    /* insert all rows in db */
    var rowNumber = 0
    for (row <- rows) {
      rowNumber += 1

      if (row.size != csvColumnNames.size) {

        /* CSV row column count does not match CSV header column count */
        splog.warning(s"Skipped row because of column count mismatch, rowNumber=$rowNumber")

      } else {

        val insertValues = row.view.zipWithIndex.flatMap({ case (value, index) =>

          val columnName = csvColumnNames(index)
          if (columnName == skippedColumnName) {

            /* Column is undefined in DTD */
            None

          } else {

            /* extract type from table definition for column name */
            val dbtype = tableDefinition.name2dbType(columnName)

            dbtype match {
              case VaryingCharactersDbType | CharactersDbType | DateDbType => Some("'" + sanitizeDbValue(value) + "'")
              case IntegerDbType => Some(value)
              case _ => throw new RuntimeException("unhandled db type")
            }

          }

        }).mkString(", ")

        db.addBatch(s"insert into recipients ($insertColumns) values ($insertValues)")

        /* execute batch each X records */
        if (rowNumber % insertBatchSize == 0) {
          db.executeBatch()
          splog.info(s"Executing batch insert, count=$rowNumber")
        }
      }
    }

    val recipientsCount = db.selectCount(s"select count(*) from recipients")
    splog.info(s"Done with SQL inserts, recipientsDbCount=$recipientsCount")
  }

  private def csvStreamFromS3Content(bucket: String, key: String) = {
    splog.info(s"Streaming CSV content from S3 bucket=$bucket key=$key")
    val reader = s3Adapter.getObjectContentAsStreamReader(bucket, key)
    val csvStream = csvAdapter.parseReaderAsStream(reader)
    if (csvStream.isEmpty) throw new RuntimeException("csv stream is empty")
    csvStream
  }

  override def handleTask(params: ActivityParameters) = {

    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    /* extract and validate params */
    val (bucket, key, filename, tableDefinition) = getParams(params)

    val csvStream = csvStreamFromS3Content(bucket, key)

    splog.info("Creating DB file")
    val db = liteDbAdapter.create(filename)

    try {

      splog.info("Creating DB schema")
      db.execute(tableDefinition.toSql)

      splog.info("Writing CSV records to DB")
      writeCsvStreamToDb(csvStream, tableDefinition, db)

      db.commit()

    } finally {
      db.close()
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
