package com.balihoo.fulfillment.workers.ses

import java.io.Reader
import java.net.URI
import java.text.SimpleDateFormat

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import com.balihoo.fulfillment.workers.datastore.{DatabaseColumnDefinition, DatabaseTypes, DatabaseTableDefinition}
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}

/**
 * Worker that creates a database file from a csv file, based on a dtd.
 * TODO (jmelanson) use URL or URI ActivityParameter type.
 */
abstract class AbstractDatabaseCreate extends FulfillmentWorker {

  this: LoggingWorkflowAdapter
    with S3AdapterComponent
    with CsvAdapterComponent
    with FilesystemAdapterComponent
    with LightweightDatabaseAdapterComponent =>

  /** Table definition JSON format. */
  implicit val tableColumnDefinition = Json.format[DatabaseColumnDefinition]
  implicit val tableDefinitionFormat = Json.format[DatabaseTableDefinition]

  val skippedColumnName = "__skipped_column__"
  val insertBatchSize = 100000
  val csvLastModifiedAttribute = "csvlastmodified"
  val csvLastModifiedDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
  val sqlDateParser = new SimpleDateFormat("yyyy-MM-dd")
  def s3dir = swfAdapter.config.getString("s3dir")
  def s3bucket = swfAdapter.config.getString("s3bucket")

  object EmailDatabaseSchemaDefinitionParameter
    extends ObjectParameter(
      "dtd",
      "JSON configuration document that describes the columns: SQL data type, name mappings from source to canonical, indexes, etc. (more to come)",
      required = true,
      properties = List(
        new StringParameter("name", "database table name", required = false),
        new ArrayParameter("columns", "database columns definitions",
          new ObjectParameter("", "", List(
            new StringParameter("name", "table column's name", minLength = Some(1)),
            new StringParameter("type", "table column's type", minLength = Some(1)),
            new StringParameter("source", "source csv header name", minLength = Some(1)),
            new StringParameter("index", "table column's index name or type", required = false, minLength = Some(1))
          )),
          minItems = 1,
          uniqueItems = true
        )
      )
    )

  override lazy val getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      List(
        new UriParameter("source", "URL that indicates where the source data is downloaded from (S3)"),
        new StringParameter("dbname", "Name of the lightweight database file that will be generated", minLength = Some(1)),
        EmailDatabaseSchemaDefinitionParameter
      ),
      new StringResultType("URL to the lightweight database file"),
      "Insert all records from a CSV file to a lightweight database file, according to a DTD")
  }

  /**
   * Extract, validate and return parameters for this task.
   */
  private def getParams(parameters: ActivityArgs) = {

    splog.info("Parsing parameters source, dbname and dtd")

    val maybeSource = parameters.get[URI]("source")
    val maybeDbName = parameters.get[String]("dbname")
    val maybeDtd = parameters.get[ActivityArgs]("dtd")

    if (!maybeSource.isDefined || maybeSource.get.toString.trim.isEmpty) throw new IllegalArgumentException("source parameter is empty")
    val sourceUri = maybeSource.get
    if (sourceUri.getScheme.toLowerCase != "s3") throw new IllegalArgumentException("source protocol is unsupported for now")
    if (!maybeDbName.isDefined || maybeDbName.get.trim.isEmpty) throw new IllegalArgumentException("dbname parameter is empty")
    if (!maybeDtd.isDefined) throw new IllegalArgumentException("dtd parameter is empty")

    val tableDefinition = Try(Json.parse(maybeDtd.get.input).as[DatabaseTableDefinition]) match {
      case Success(td) => td
      case Failure(t) => throw new IllegalArgumentException("invalid DTD")
    }

    splog.debug(s"csv header name -> column name : ${tableDefinition.source2name}")
    splog.debug(s"column name -> column sql type : ${tableDefinition.name2type}")
    splog.debug(s"table ddl sql : ${tableDefinition.tableCreateSql}")

    (sourceUri.getHost, sourceUri.getPath.substring(1), maybeDbName.get, tableDefinition)
  }

  /**
   * Event handler for a row that contains wrong columns count.
   */
  private def handleBadRow(rowNum: Integer) = {
    if (swfAdapter.config.getOrElse("failOnBadCsvRecord", default = true)) {
      throw new RuntimeException(s"CSV contains bad row, aborting! rowNumber=$rowNum")
    } else {
      splog.warning(s"Skipping bad record, rowNumber=$rowNum")
    }
  }

  /**
   * Writes the stream of CSV records (list of strings) into the given db.
   * A table definition is required for mapping types and column names between csv and db.
   */
  private def writeCsvStreamToDb(csvStream: Stream[List[String]], tableDefinition: DatabaseTableDefinition, db: LightweightDatabase) = {

    /*
      Map CSV headers to column names, using dtd.
      If CSV header not found, add it as a 'skipped' column.
     */
    val csvColumnNames = csvStream.head.map(header => tableDefinition.source2name.getOrElse(header.toLowerCase, skippedColumnName))
    if (csvColumnNames.isEmpty) throw new RuntimeException("Wrong CSV / DTD mapping: no columns found!")
    splog.debug(s"Ordered CSV columns : $csvColumnNames")

    val nonSkippedColumns = csvColumnNames.filterNot(_ == skippedColumnName).map("\"" + _ + "\"")
    val sqlColumnNames = nonSkippedColumns.mkString(", ")
    splog.debug(s"Ordered SQL insert columns : $sqlColumnNames")

    /* skip header, work on rows from now on */
    val rows = csvStream.drop(1)
    if (rows.isEmpty) throw new RuntimeException("CSV stream has headers, but no rows")

    /* generates param size */
    val params = ("?" * nonSkippedColumns.size).mkString(", ")
    val dbBatch = db.batch(s"""insert into "${tableDefinition.getName}" ($sqlColumnNames) values ($params)""")

    /**
     * @return type-value tuple sequence for known column names (based on index)
     */
    def extractTypesAndValues(row: List[String]) = {
      row.view.zipWithIndex.flatMap({ case (value, index) =>
        csvColumnNames(index) match {
          case `skippedColumnName` => None /* Column is undefined in DTD */
          case columnName => Some(tableDefinition.name2type(columnName) -> value)
        }
      })
    }

    /**
     * Add a parameter to the prepared statement based on db type and index.
     */
    def addSqlParam(sqlParamIndex: Int, dbType: DatabaseTypes.DataType, value: String) = {
      if (Option(value).isEmpty || value.trim.isEmpty)
        dbBatch.param(sqlParamIndex, None)
      else dbType match {
        case DatabaseTypes.Text => dbBatch.param(sqlParamIndex, Some(value))
        case DatabaseTypes.Integer => dbBatch.param(sqlParamIndex, Some(value.toLong))
        case DatabaseTypes.Date =>
          val aDate = new java.sql.Date(sqlDateParser.parse(value).getTime)
          dbBatch.param(sqlParamIndex, Some(aDate))
        /** TODO (jmelanson) support timestamp (need requirements) */
        case DatabaseTypes.Timestamp => dbBatch.param(sqlParamIndex, Some(value))
        case DatabaseTypes.Boolean => dbBatch.param(sqlParamIndex, Some(value.toBoolean))
        case DatabaseTypes.Real => dbBatch.param(sqlParamIndex, Some(value.toDouble))
        case unhandledDbType => throw new RuntimeException(s"unhandled db type=$unhandledDbType")
      }
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

    splog.info(s"Creating indexes...")
    tableDefinition.uniqueIndexCreateSql.foreach(db.execute)
    tableDefinition.simpleIndexCreateSql.foreach(db.execute)

    splog.info(s"Testing DB...")
    val recipientsCount = db.selectCount(s"""select count(*) from "${tableDefinition.getName}"""")
    splog.info(s"Done with SQL inserts, recipientsDbCount=$recipientsCount")
  }

  def createDb(db: LightweightDatabase, tableDefinition: DatabaseTableDefinition, csvReader: Reader) = {
    splog.debug("Creating DB schema")
    db.execute(tableDefinition.tableCreateSql)

    splog.debug("Writing CSV records to DB")
    val csvStream = csvAdapter.parseReaderAsStream(csvReader).get
    writeCsvStreamToDb(csvStream, tableDefinition, db)

    db.commit()
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {

    val (bucket, key, dbName, tableDefinition) = getParams(params)

    val dbS3Key = {
      val gzDbName = if (dbName.endsWith(".gz")) dbName else s"$dbName.gz"
      s"$s3dir/$gzDbName"
    }

    val csvMeta = workerResource(s3Adapter.getMeta(bucket, key).get)
    val dbMetaTry = s3Adapter.getMeta(dbS3Key)
    val useCache = dbMetaTry
      .map(_.userMetaData(csvLastModifiedAttribute))
      .map(csvLastModifiedDateFormat.parse)
      .map(_.getTime / 1000)
      .map(_ >= csvMeta.lastModified.getTime / 1000) /* don't look at the millis (we don't store it) */
      .getOrElse(false)

    if (dbMetaTry.isSuccess) workerResource(dbMetaTry.get)


    val uri = if (useCache) {

      val s3uri = dbMetaTry.get.s3Uri.toString
      splog.info(s"Returning cached database at uri=$s3uri")
      s3uri

    } else {

      splog.info("Creating database with csv data...")
      val dbTempFile = workerFile(filesystemAdapter.newTempFile(dbName + ".sqlite"))
      val db = workerResource(liteDbAdapter.create(dbTempFile.getAbsolutePath))
      val csvInputStreamReader = workerResource(filesystemAdapter.newReader(csvMeta.getContentStream))
      createDb(db, tableDefinition, csvInputStreamReader)

      splog.info("Compressing database...")
      val gzDbFile = filesystemAdapter.gzip(dbTempFile)
      val lastModifiedValue = csvLastModifiedDateFormat.format(csvMeta.lastModified)
      val metaData = Map(csvLastModifiedAttribute -> lastModifiedValue)

      splog.info("Uploading database...")
      val dbUri =
        s3Adapter
          .upload(dbS3Key, gzDbFile, userMetaData = metaData)
          .map(_.toString)
          .get

      dbUri.toString

    }

    getSpecification.createResult(uri)
  }
}

class DatabaseCreate(override val _cfg: PropertiesLoader, override val _splog: Splogger) extends AbstractDatabaseCreate
  with ScalaCsvAdapterComponent
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent
  with FilesystemAdapterComponent
  with SQLiteLightweightDatabaseAdapterComponent {
    override val s3Adapter = new S3Adapter(_cfg, _splog)
}

object db_create extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker =
    new DatabaseCreate(cfg, splog)
}
