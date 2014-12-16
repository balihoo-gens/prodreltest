package com.balihoo.fulfillment.workers.ses

import java.io.Reader
import java.net.URI
import java.text.SimpleDateFormat

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}

/**
 * Worker that creates a database file from a csv file, based on a dtd.
 * TODO (jmelanson) use URL or URI ActivityParameter type.
 */
abstract class AbstractEmailCreateDBWorker extends FulfillmentWorker {

  this: LoggingWorkflowAdapter
    with S3AdapterComponent
    with CsvAdapterComponent
    with FilesystemAdapterComponent
    with LightweightDatabaseAdapterComponent =>

  /** Table definition JSON format. */
  implicit val tableColumnDefinition = Json.format[ColumnDefinition]
  implicit val tableDefinitionFormat = Json.format[TableDefinition]

  val skippedColumnName = "__skipped_column__"
  val insertBatchSize = 100000
  val csvLastModifiedAttribute = "csvlastmodified"
  val csvLastModifiedDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
  val sqlDateParser = new SimpleDateFormat("yyyy-MM-dd")
  def s3dir = swfAdapter.config.getString("s3dir")
  def s3bucket = swfAdapter.config.getString("s3bucket")

  object EmailDatabaseSchemaDefinitionParameter$
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
        EmailDatabaseSchemaDefinitionParameter$
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

    val tableDefinition = Try(Json.parse(maybeDtd.get.input).as[TableDefinition]) match {
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
  private def writeCsvStreamToDb(csvStream: Stream[List[String]], tableDefinition: TableDefinition, db: LightweightDatabase) = {

    val time = System.currentTimeMillis()

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
    def addSqlParam(sqlParamIndex: Int, dbType: DataTypes.DataType, value: String) = dbType match {
      case DataTypes.Text => dbBatch.param(sqlParamIndex, value)
      case DataTypes.Integer => dbBatch.param(sqlParamIndex, value.toLong)
      case DataTypes.Date => dbBatch.param(sqlParamIndex, new java.sql.Date(sqlDateParser.parse(value).getTime))
      /** TODO (jmelanson) support timestamp (need requirements) */
      case DataTypes.Timestamp => dbBatch.param(sqlParamIndex, value)
      case DataTypes.Boolean => dbBatch.param(sqlParamIndex, value.toBoolean)
      case DataTypes.Real => dbBatch.param(sqlParamIndex, value.toDouble)
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

    splog.info(s"Creating indexes...")
    tableDefinition.uniqueIndexCreateSql.foreach(db.execute)
    tableDefinition.simpleIndexCreateSql.foreach(db.execute)

    splog.info(s"Testing DB...")
    val recipientsCount = db.selectCount(s"""select count(*) from "${tableDefinition.getName}"""")
    splog.info(s"Done with SQL inserts, recipientsDbCount=$recipientsCount")

    splog.debug("csv to sql time=" + (System.currentTimeMillis() - time))
  }

  def createDb(db: LightweightDatabase, tableDefinition: TableDefinition, csvReader: Reader) = {
    splog.debug("Creating DB schema")
    db.execute(tableDefinition.tableCreateSql)

    splog.debug("Writing CSV records to DB")
    val csvStream = csvAdapter.parseReaderAsStream(csvReader).get
    writeCsvStreamToDb(csvStream, tableDefinition, db)

    db.commit()
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {

    val (bucket, key, dbName, tableDefinition) = getParams(params)
    val dbS3Key = s"$s3dir/$dbName"

    val csvMeta = s3Adapter.getMeta(bucket, key).get
    val dbMetaTry = s3Adapter.getMeta(dbS3Key)
    val useCache = dbMetaTry
      .map(_.userMetaData(csvLastModifiedAttribute))
      .map(csvLastModifiedDateFormat.parse)
      .map(_.getTime / 1000)
      .map(_ >= csvMeta.lastModified.getTime / 1000) /* don't look at the millis (we don't store it) */
      .getOrElse(false)

    if (dbMetaTry.isSuccess) dbMetaTry.get.close()

    getSpecification.createResult(
      if (useCache) {

        csvMeta.close()
        val s3uri = dbMetaTry.get.s3Uri.toString
        splog.info(s"Re-using existing database (cached) uri=$s3uri")
        s3uri

      } else {

        splog.info("Generating database from csv...")

        val dbTempFile = filesystemAdapter.newTempFile(dbName + ".sqlite")
        val db = liteDbAdapter.create(dbTempFile.getAbsolutePath)
        val csvInputStreamReader = filesystemAdapter.newReader(csvMeta.getContentStream)

        try {

          createDb(db, tableDefinition, csvInputStreamReader)

          val lastModifiedValue = csvLastModifiedDateFormat.format(csvMeta.lastModified)
          val metaData = Map(csvLastModifiedAttribute -> lastModifiedValue)
          val dbUri =
            s3Adapter
              .upload(dbS3Key, dbTempFile, userMetaData = metaData)
              .map(_.toString)
              .get

          dbUri.toString

        } finally {
          db.close()
          csvInputStreamReader.close()
          csvMeta.close()
          dbTempFile.delete()
        }
      }
    )
  }
}


/**
 * Supported database types enumeration.
 */
object DataTypes {
  sealed abstract class DataType(val aliases: String*) {
    val regex = aliases
      .map(_.replaceAll("\\s", "\\\\s"))
      .mkString("(", "|", ")\\s*([(](\\d+)[)]){0,1}")
      .r
  }
  case object Text extends DataType("character", "varchar", "varying character", "char", "nchar", "native character", "nvarchar", "text", "clob")
  case object Integer extends DataType("int", "integer", "tinyint", "smallint", "mediumint", "bigint", "unsigned big int", "int2", "int8")
  case object Real extends DataType("real", "double", "double precision", "float")
  case object Date extends DataType("date")
  case object Boolean extends DataType("boolean")
  case object Timestamp extends DataType("datetime", "timestamp")

  val AllSupportedAliases = Boolean.aliases ++ Date.aliases ++ Integer.aliases ++ Real.aliases ++ Text.aliases ++ Timestamp.aliases
}

/**
 * Simple SQL column definition.
 */
case class ColumnDefinition(name: String, `type`: String, source: String, index: Option[String] = None) {

  /** members as lower case */

  val getName = name.toLowerCase
  val getType = `type`.toLowerCase
  val getSource = source.toLowerCase
  def getIndex = index.get.toLowerCase

  /** Simple column creation sql statement. */
  val columnCreateSql = "\"" + getName + "\" " + getType

  /** Column is indexed. */
  val isIndexed = index.isDefined

  /** Column part of primary key. */
  def isPrimaryKey = index.fold(false) {
    case idx if getIndex matches "primary key|pk|primary|primarykey" => true
    case _ => false
  }

  /** Column has unique index. */
  def isUniqueKey = index.fold(false) {
    case idx if getIndex matches "unique" => true
    case _ => false
  }

  /** Index is simple. */
  def isSimpleIndex = isIndexed && !(isUniqueKey || isPrimaryKey)

  /**
   * @return Data type from the raw column type.
   */
  def dataType = getType match {
    case DataTypes.Text.regex(keywords, _, size) => DataTypes.Text
    case DataTypes.Integer.regex(keywords, _, size) => DataTypes.Integer
    case DataTypes.Real.regex(keywords, _, size) => DataTypes.Real
    case DataTypes.Date.regex(keywords, _, size) => DataTypes.Date
    case DataTypes.Boolean.regex(keywords, _, size) => DataTypes.Boolean
    case DataTypes.Timestamp.regex(keywords, _, size) => DataTypes.Timestamp
    case _ => throw new RuntimeException(s"unsupported db data type [${`type`}]")
  }
}

/**
 * Simple SQL table definition.
 */
case class TableDefinition(columns: Seq[ColumnDefinition] = Seq.empty, name: Option[String] = Some("recipients")) {

  /** Source to name mapping. */
  val source2name = columns.map(col => col.getSource -> col.getName).toMap

  /** Name to data type mapping. */
  val name2type = columns.map(col => col.getName -> col.dataType).toMap

  /** Index names to indexed column definitions. */
  val indexedColumns = columns.filter(_.isIndexed)

  /** Primary key column definitions */
  val primaryKeyIndexColumns = indexedColumns.filter(_.isPrimaryKey)

  /** Unique indexes. */
  val uniqueIndexColumns = indexedColumns.filter(_.isUniqueKey)

  /** Simple indexes. */
  val simpleIndexColumns = indexedColumns.filter(_.isSimpleIndex)

  /** Primary key creation sql statement. */
  val primaryKeyCreateSql = primaryKeyIndexColumns.map("\"" + _.getName + "\"").mkString("primary key (", ", ", ")")

  /** Table name to use. */
  val getName = name.getOrElse("recipients")

  /** Return a data definition SQL statement from this table definition. */
  val tableCreateSql = {
    val columnsCreateSql = columns.map(_.columnCreateSql)
    val allDefinitions = if (primaryKeyIndexColumns.isEmpty) columnsCreateSql else columnsCreateSql :+ primaryKeyCreateSql
    allDefinitions.mkString(s"""create table "$getName" (""", ", ", ")")
  }

  /** Return a list of create SQL statement for unique indexes defined in this table definition. */
  val uniqueIndexCreateSql = uniqueIndexColumns.map({ column =>
    s"""create unique index "${column.getName}_unique_idx" on "$getName" ("${column.getName}")"""
  }).toSeq

  /** Return a list of create SQL statement for simple indexes defined in this table definition. */
  val simpleIndexCreateSql = simpleIndexColumns
    .groupBy(_.getIndex)
    .map({ case (indexName, indexColumnDefinitions) =>
      val indexColumns = indexColumnDefinitions.map("\"" + _.getName + "\"").mkString(", ")
      s"""create index "$indexName" on "$getName" ($indexColumns)"""
    }).toSeq

}

class EmailCreateDBWorker(override val _cfg: PropertiesLoader, override val _splog: Splogger) extends AbstractEmailCreateDBWorker
  with ScalaCsvAdapterComponent
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent
  with FilesystemAdapterComponent
  with SQLiteLightweightDatabaseAdapterComponent {
    override val s3Adapter = new S3Adapter(_cfg, _splog)
}

object email_createdb extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker =
    new EmailCreateDBWorker(cfg, splog)
}
