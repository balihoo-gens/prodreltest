package com.balihoo.fulfillment.workers

import java.io.{InputStreamReader, File}
import java.net.URISyntaxException
import java.text.SimpleDateFormat

import com.balihoo.fulfillment.adapters._
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import org.specs2.specification.Scope
import play.api.libs.json.Json

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class TestEmailCreateDBWorker extends Specification with Mockito {

  "EmailCreateDBWorker.getSpecification" should {
    "return a valid specification" in new TestContext {
      val spec = worker.getSpecification
      spec must beAnInstanceOf[ActivitySpecification]
      spec.params.size must beEqualTo(3)
      spec.params.count(_.name == "source") must beEqualTo(1)
      spec.params.count(_.name == "dbname") must beEqualTo(1)
      spec.params.count(_.name == "dtd") must beEqualTo(1)
      spec.result must beAnInstanceOf[StringActivityResult]
      spec.description must not(beEmpty)
    }
  }

  "EmailCreateDBWorker.handleTask" should {
    "fail task if source param missing" in new TestContext {
      val activityParameter = new ActivityParameters(Map())
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if source param is invalid uri" in new TestContext {
      val activityParameter = new ActivityParameters(Map("source" -> "invalid uri"))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[URISyntaxException]
    }
    "fail task if dbname param missing" in new TestContext {
      val activityParameter = new ActivityParameters(Map("source" -> data.source, "dtd" -> data.dtd))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if dtd param missing" in new TestContext {
      val activityParameter = new ActivityParameters(Map("source" -> data.source, "dbname" -> data.dbname))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if dtd param is invalid" in new TestContext {
      val activityParameter = new ActivityParameters(Map("source" -> data.source, "dtd" -> data.invalidDtd, "dbname" -> data.dbname))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if protocol is unsupported" in new TestContext {
      val activityParameter = new ActivityParameters(Map(
        "source" -> data.sourceWithInvalidScheme,
        "dbname" -> data.dbname,
        "dtd" -> data.dtd))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if csv stream empty" in new TestContext {
      givenReader()
      givenCsvStream(Stream.empty)
      givenTempDbFile()
      givenLiteDb()
      val activityParameter = new ActivityParameters(Map(
        "source" -> data.source,
        "dbname" -> data.dbname,
        "dtd" -> data.dtd))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[RuntimeException]
    }
    "fail task if csv stream has no records" in new TestContext {
      givenReader()
      givenCsvStream(data.header #:: Stream.empty)
      givenTempDbFile()
      givenLiteDb()
      val activityParameter = new ActivityParameters(Map(
        "source" -> data.source,
        "dbname" -> data.dbname,
        "dtd" -> data.dtd))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[RuntimeException]
    }
    "fail task if csv stream has bad records and failOnBadRecord is true" in new TestContext {
      givenFailOnBadRecord()
      givenReader()
      givenCsvStream(data.header #:: Stream.empty)
      givenTempDbFile()
      givenLiteDb()
      val activityParameter = new ActivityParameters(Map(
        "source" -> data.source,
        "dbname" -> data.dbname,
        "dtd" -> data.dtd))
      Try(worker.handleTask(activityParameter)) must beFailedTry.withThrowable[RuntimeException]
    }
    "complete task if db file could be created from csv and uploaded to s3" in new TestContext {
      givenReader()
      givenCsvStream()
      givenTempDbFile()
      givenLiteDb()
      val activityParameter = new ActivityParameters(Map(
        "source" -> data.source,
        "dbname" -> data.dbname,
        "dtd" -> data.dtd))

      worker.handleTask(activityParameter)

      there was one(worker.dbMock).execute("create table \"recipients\" (" +
        "\"locationid\" integer, " +
        "\"recipientid\" varchar(100), " +
        "\"email\" varchar(100), " +
        "\"firstname\" varchar(50), " +
        "\"lastname\" varchar(50), " +
        "\"birthday\" date, " +
        "primary key (\"locationid\", \"recipientid\"))")
      there was one(worker.dbMock).batch("insert into \"recipients\" " +
        "(\"recipientid\", \"locationid\", \"email\", \"firstname\", \"lastname\", \"birthday\") " +
        "values (?, ?, ?, ?, ?, ?)")

      there was one(worker.dbBatchMock).param(1, "b")
      there was one(worker.dbBatchMock).param(2, 2.toLong)
      there was one(worker.dbBatchMock).param(3, "rafael@nike.com")
      there was one(worker.dbBatchMock).param(4, "rafael")
      there was one(worker.dbBatchMock).param(5, "nadal")
      there was one(worker.dbBatchMock).param(6, new java.sql.Date(sqlDateParser.parse("1986-06-03").getTime))

      there was one(worker.dbBatchMock).param(1, "a")
      there was one(worker.dbBatchMock).param(2, 1.toLong)
      there was one(worker.dbBatchMock).param(3, "roger@nike.com")
      there was one(worker.dbBatchMock).param(4, "roger")
      there was one(worker.dbBatchMock).param(5, "federer")
      there was one(worker.dbBatchMock).param(6, new java.sql.Date(sqlDateParser.parse("1981-08-08").getTime))

      there was one(worker.dbBatchMock).param(1, "c")
      there was one(worker.dbBatchMock).param(2, 3.toLong)
      there was one(worker.dbBatchMock).param(3, "novak@uniqlo.com")
      there was one(worker.dbBatchMock).param(4, "novak")
      there was one(worker.dbBatchMock).param(5, "djokovic")
      there was one(worker.dbBatchMock).param(6, new java.sql.Date(sqlDateParser.parse("1987-05-22").getTime))

      there was three(worker.dbBatchMock).add()
      there was two(worker.dbBatchMock).execute()

      there was one(worker.dbMock).commit()
      there was one(worker.dbMock).execute("create unique index \"email_unique_idx\" on \"recipients\" (\"email\")")
      there was one(worker.dbMock).execute("create index \"fullname\" on \"recipients\" (\"firstname\", \"lastname\")")
      there was one(worker.dbMock).execute("create index \"bday\" on \"recipients\" (\"birthday\")")

      there was one(worker.dbMock).close()
      there was one(worker.readerMock).close()
      there was one(worker.s3Adapter).putPublic(===("mock"), beMatching(s"mock/\\d+/${data.dbname}"), any[File])
      there was one(worker.dbFileMock).delete()

      /* make sure output is complete s3 url */
      worker.test_complete_result must beMatching(s"s3://mock/mock/\\d+/" + data.dbname)
    }
  }

  class TestContext extends Scope {

    val sqlDateParser = new SimpleDateFormat("yyyy-MM-dd")

    object data {
      val s3bucket = "balihoo.fulfillment.stuff"
      val s3key = "my/key/file.csv"
      val source = s"s3://$s3bucket/$s3key"
      val sourceWithInvalidScheme = s"http://$s3bucket/$s3key"
      val dbname = "test.db"
      val dtd = Json.obj("columns" -> Json.arr(
        Json.obj(
          "name" -> "locationId",
          "type" -> "integer",
          "source" -> "STORENUM",
          "index" -> "primary"
        ),
        Json.obj(
          "name" -> "recipientId",
          "type" -> "varchar(100)",
          "source" -> "RECIPIENT",
          "index" -> "primary"
        ),
        Json.obj(
          "name" -> "email",
          "type" -> "varchar(100)",
          "source" -> "EMAILADDR",
          "index" -> "unique"
        ),
        Json.obj(
          "name" -> "firstName",
          "type" -> "varchar(50)",
          "source" -> "FNAME",
          "index" -> "fullname"
        ),
        Json.obj(
          "name" -> "lastName",
          "type" -> "varchar(50)",
          "source" -> "LNAME",
          "index" -> "fullname"
        ),
        Json.obj(
          "name" -> "birthday",
          "type" -> "date",
          "source" -> "BDAY",
          "index" -> "bday"
        )
      ))
      val invalidDtd = Json.obj()
      val header = List("RECIPIENT", "STORENUM", "emailaddr", "UNUSED", "FNAME", "LNAME", "TYPE", "BDAY")
      val roger = List("a", "1", "roger@nike.com", "some", "roger", "federer", "a", "1981-08-08")
      val rafael = List("b", "2", "rafael@nike.com", "some", "rafael", "nadal", "a", "1986-06-03")
      val novak = List("c", "3", "novak@uniqlo.com", "some", "novak", "djokovic", "a", "1987-05-22")
      val badRecordNovak = List("c", "3", "some", "novak", "djokovic", "a", "1987-05-22")
      val csvStream = header #:: rafael #:: roger #:: novak #:: Stream.empty
      val targetBucket= "test.bucket"
      val targetDir = "somedir"
    }

    val worker = new AbstractEmailCreateDBWorker
      with LoggingWorkflowAdapterTestImpl
      with S3AdapterComponent
      with CsvAdapterComponent
      with FilesystemAdapterComponent
      with LightweightDatabaseAdapterComponent {

      override val s3Adapter = mock[AbstractS3Adapter]
      override val csvAdapter = mock[CsvAdapter]
      override val liteDbAdapter = mock[LightweightDatabaseAdapter]
      override val filesystemAdapter = mock[FilesystemAdapter]
      val readerMock = mock[InputStreamReader]
      val dbFileMock = mock[File]
      val dbMock = mock[LightweightDatabase with LightweightFileDatabase]
      val dbBatchMock = mock[DbBatch]
      override val insertBatchSize = 2
      var test_complete_result = ""
      override def completeTask(result: String) = {
        test_complete_result = result
      }
    }

    def givenFailOnBadRecord(value: Boolean = true) = {
      worker.swfAdapter.config.getOrElse("failOnBadRecord", default = true) returns value
    }

    def givenReader() = {
      worker.s3Adapter.getObjectContentAsReader(data.s3bucket, data.s3key) returns worker.readerMock
    }

    def givenCsvStream(stream: Stream[List[String]] = data.csvStream) = {
      worker.csvAdapter.parseReaderAsStream(worker.readerMock) returns stream
    }

    def givenLiteDb() = {
      worker.dbMock.batch(anyString) returns worker.dbBatchMock
      worker.liteDbAdapter.create(any[File]) returns worker.dbMock
    }

    def givenTempDbFile() = {
      worker.filesystemAdapter.newTempFile(===("email-createdb-" + data.dbname), ===(".sqllite")) returns worker.dbFileMock
    }

  }

  "ColumnDefinition" should {
    "detect text types" in new DbTestContext {
      ColumnDefinition("aname", "nvarchar", "asource").dataType must beEqualTo(DataTypes.Text)
      ColumnDefinition("aname", "char(3)", "asource").dataType must beEqualTo(DataTypes.Text)
      ColumnDefinition("aname", "varchar(100)", "asource").dataType must beEqualTo(DataTypes.Text)
      ColumnDefinition("aname", "text", "asource").dataType must beEqualTo(DataTypes.Text)
    }
    "detect integer types" in new DbTestContext {
      ColumnDefinition("aname", "int", "asource").dataType must beEqualTo(DataTypes.Integer)
      ColumnDefinition("aname", "integer", "asource").dataType must beEqualTo(DataTypes.Integer)
      ColumnDefinition("aname", "bigint", "asource").dataType must beEqualTo(DataTypes.Integer)
      ColumnDefinition("aname", "smallint", "asource").dataType must beEqualTo(DataTypes.Integer)
    }
    "detect real types" in new DbTestContext {
      ColumnDefinition("aname", "real", "asource").dataType must beEqualTo(DataTypes.Real)
      ColumnDefinition("aname", "float", "asource").dataType must beEqualTo(DataTypes.Real)
      ColumnDefinition("aname", "double", "asource").dataType must beEqualTo(DataTypes.Real)
    }
    "detect date types" in new DbTestContext {
      ColumnDefinition("aname", "date", "asource").dataType must beEqualTo(DataTypes.Date)
    }
    "detect timestamp types" in new DbTestContext {
      ColumnDefinition("aname", "datetime", "asource").dataType must beEqualTo(DataTypes.Timestamp)
      ColumnDefinition("aname", "timestamp", "asource").dataType must beEqualTo(DataTypes.Timestamp)
    }
  }

  "TableDefinition" should {
    "populate source to name map" in new DbTestContext {
      val result = data.tableDefinition.source2name
      result must beAnInstanceOf[Map[String, String]]
      result.size must beEqualTo(data.tableDefinition.columns.size)
      result.getOrElse("SOMEINT".toLowerCase, "") must beEqualTo("someint")
      result.getOrElse("SOMEINT2".toLowerCase, "") must beEqualTo("someint2")
      result.getOrElse("SOMESTRING".toLowerCase, "") must beEqualTo("somestring")
      result.getOrElse("SOMEDATE".toLowerCase, "") must beEqualTo("somedate")
      result.getOrElse("SOMESTRING2".toLowerCase, "") must beEqualTo("somestring2")
      result.getOrElse("SOMESTRING3".toLowerCase, "") must beEqualTo("somestring3")
      result.getOrElse("SOMEBOOL".toLowerCase, "") must beEqualTo("somebool")
      result.getOrElse("SOMETIMESTAMP".toLowerCase, "") must beEqualTo("sometimestamp")
      result.getOrElse("SOMEREAL".toLowerCase, "") must beEqualTo("somereal")
    }
    "populate names to db types map" in new DbTestContext {
      val result = data.tableDefinition.name2type
      result must beAnInstanceOf[Map[String, String]]
      result.size must beEqualTo(data.tableDefinition.columns.size)
      result.getOrElse("someint", "") must beEqualTo(DataTypes.Integer)
      result.getOrElse("someint2", "") must beEqualTo(DataTypes.Integer)
      result.getOrElse("somestring", "") must beEqualTo(DataTypes.Text)
      result.getOrElse("somedate", "") must beEqualTo(DataTypes.Date)
      result.getOrElse("somestring2", "") must beEqualTo(DataTypes.Text)
      result.getOrElse("somestring3", "") must beEqualTo(DataTypes.Text)
      result.getOrElse("somebool", "") must beEqualTo(DataTypes.Boolean)
      result.getOrElse("sometimestamp", "") must beEqualTo(DataTypes.Timestamp)
      result.getOrElse("somereal", "") must beEqualTo(DataTypes.Real)
    }
    "generate appropriate table creation sql statement" in new DbTestContext {
      val result = data.tableDefinition.tableCreateSql
      result must beAnInstanceOf[String]
      result must beEqualTo("create table \"recipients\" (" +
        "\"someint\" integer, " +
        "\"someint2\" integer, " +
        "\"somestring\" varchar(50), " +
        "\"somedate\" date, " +
        "\"somestring2\" char(3), " +
        "\"somestring3\" char(3), " +
        "\"somebool\" boolean, " +
        "\"sometimestamp\" timestamp, " +
        "\"somereal\" double, " +
        "primary key (\"someint\", \"someint2\"))")
    }
    "generate appropriate unique indexes creation sql statements" in new DbTestContext {
      val uniques = data.tableDefinition.uniqueIndexCreateSql
      uniques must beAnInstanceOf[Seq[String]]
      uniques must beEqualTo(Seq("create unique index \"somestring_unique_idx\" on \"recipients\" (\"somestring\")"))
    }
    "generate appropriate simple indexes creation sql statements" in new DbTestContext {
      val simples = data.tableDefinition.simpleIndexCreateSql
      simples must beAnInstanceOf[Seq[String]]
      simples must beEqualTo(Seq("create index \"an_index_name\" on \"recipients\" (\"somestring2\", \"somestring3\")"))
    }
  }

  class DbTestContext extends Scope {
    object data {
      val col1 = ColumnDefinition("someint", "integer", "SOMEINT", Some("primary key"))
      val col2 = ColumnDefinition("someint2", "integer", "SOMEINT2", Some("primary key"))
      val col3 = ColumnDefinition("somestring", "varchar(50)", "SOMESTRING", Some("unique"))
      val col4 = ColumnDefinition("somedate", "date", "SOMEDATE")
      val col5 = ColumnDefinition("somestring2", "char(3)", "SOMESTRING2", Some("an_index_name"))
      val col6 = ColumnDefinition("somestring3", "char(3)", "SOMESTRING3", Some("an_index_name"))
      val col7 = ColumnDefinition("somebool", "boolean", "SOMEBOOL")
      val col8 = ColumnDefinition("sometimestamp", "timestamp", "SOMETIMESTAMP")
      val col9 = ColumnDefinition("somereal", "double", "SOMEREAL")
      val tableDefinition =
        TableDefinition(
          Seq(
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            col7,
            col8,
            col9
          )
        )
    }
  }
}
