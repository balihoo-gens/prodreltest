package com.balihoo.fulfillment.workers.datastore

import java.io.{File, InputStream, InputStreamReader}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.workers.ses.AbstractDatabaseCreate
import com.balihoo.fulfillment.workers._
import org.junit.runner._
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import org.specs2.specification.Scope
import play.api.libs.json.{JsString, Json}

import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class TestDatabaseCreate extends Specification with Mockito {

  "email create database worker" should {
    "return a valid specification" in new WithWorker {
      val spec = getSpecification
      spec must beAnInstanceOf[ActivitySpecification]
      spec.params.size must beEqualTo(3)
      spec.params.count(_.name == "source") must beEqualTo(1)
      spec.params.count(_.name == "dbname") must beEqualTo(1)
      spec.params.count(_.name == "dtd") must beEqualTo(1)
      spec.result must beAnInstanceOf[StringResultType]
      spec.description must not(beEmpty)
    }
    "fail param validation when dbname property is missing" in new WithWorker {
      val dtdJsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar"),
          Json.obj("name" -> "foz", "type" -> "char", "source" -> "baz")
        )
      )
      val input = Json.obj(
        "source" -> "aSource",
        "dtd" -> dtdJsonObj
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"dbname\"\\]\\)")
    }
    "fail param validation when dbname property is empty" in new WithWorker {
      val dtdJsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar"),
          Json.obj("name" -> "foz", "type" -> "char", "source" -> "baz")
        )
      )
      val input = Json.obj(
        "dbname" -> "",
        "source" -> "aSource",
        "dtd" -> dtdJsonObj
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dbname string \"\" is too short")
    }
    "fail param validation when source property is missing" in new WithWorker {
      val dtdJsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar"),
          Json.obj("name" -> "foz", "type" -> "char", "source" -> "baz")
        )
      )
      val input = Json.obj(
        "dbname" -> "aName",
        "dtd" -> dtdJsonObj
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"source\"\\]\\)")
    }
    "fail param validation when source property is empty" in new WithWorker {
      val dtdJsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar"),
          Json.obj("name" -> "foz", "type" -> "char", "source" -> "baz")
        )
      )
      val input = Json.obj(
        "dbname" -> "aName",
        "source" -> "",
        "dtd" -> dtdJsonObj
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/source string \"\" is too short")
    }
    "fail param validation when source property is invalid uri" in new WithWorker {
      val dtdJsonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar"),
          Json.obj("name" -> "foz", "type" -> "char", "source" -> "baz")
        )
      )
      val input = Json.obj(
        "dbname" -> "aName",
        "source" -> "some uri",
        "dtd" -> dtdJsonObj
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/source string \"some uri\" is not a valid URI")
    }
    "fail param validation when /dtd/columns property undefined" in new WithWorker {
      val input = Json.obj("source" -> "some", "dbname" -> "some", "dtd" -> Json.obj())
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd object has missing required properties")
    }
    "fail param validation when /dtd/columns property is empty array" in new WithWorker {
      val input = Json.obj("source" -> "some", "dbname" -> "some", "dtd" -> Json.obj("columns" -> Json.arr()))
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns array is too short")
    }
    "fail param validation when /dtd/columns property has non unique objects" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar"),
            Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns array must not contain duplicate elements")
    }
    "fail param validation when a /dtd/columns/source property is missing" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "foo", "type" -> "int")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0 object has missing required properties \\(\\[\"source\"\\]\\)")
    }
    "fail param validation when a /dtd/columns/type property is missing" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "foo", "source" -> "bar")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0 object has missing required properties \\(\\[\"type\"\\]\\)")
    }
    "fail param validation when a /dtd/columns/name property is missing" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("type" -> "int", "source" -> "bar")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0 object has missing required properties \\(\\[\"name\"\\]\\)")
    }
    "fail param validation when a /dtd/columns/name property is empty" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "", "type" -> "int", "source" -> "bar")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0/name string \"\" is too short")
    }
    "fail param validation when a /dtd/columns/type property is empty" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "foo", "type" -> "", "source" -> "bar")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0/type string \"\" is too short")
    }
    "fail param validation when a /dtd/columns/type property is missing" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "foo", "source" -> "bar")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0 object has missing required properties \\(\\[\"type\"\\]\\)")
    }
    "fail param validation when a /dtd/columns/source property is empty" in new WithWorker {
      val input = Json.obj(
        "source" -> "some",
        "dbname" -> "some",
        "dtd" -> Json.obj(
          "columns" -> Json.arr(
            Json.obj("name" -> "foo", "type" -> "int", "source" -> "")
          )
        )
      )
      getSpecification.getArgs(input) must throwA[ActivitySpecificationException]("/dtd/columns/0/source string \"\" is too short")
    }
    "return activity parameters given valid input" in new WithWorker {
      val dtdJonObj = Json.obj(
        "columns" -> Json.arr(
          Json.obj("name" -> "foo", "type" -> "int", "source" -> "bar", "index" -> "pk"),
          Json.obj("name" -> "foz", "type" -> "char", "source" -> "baz")
        )
      )
      val input = Json.obj(
        "source" -> "aSource",
        "dbname" -> "aName",
        "dtd" -> dtdJonObj
      )
      val result = getSpecification.getArgs(input)
      result.get[URI]("source") must beSome(new URI("aSource"))
      result.get[String]("dbname") must beSome("aName")
      val maybeDtdActivityParameters = result.get[ActivityArgs]("dtd")
      // for now, just validate that the json is the same, we have to integrate nested params in the future though...
      maybeDtdActivityParameters.get.input must beEqualTo(
        """{"columns":[{"name":"foo","type":"int","source":"bar","index":"pk"},{"name":"foz","type":"char","source":"baz"}]}""")
    }
    "fail task if csv stream has no records" in new WithWorker {
      csv_s3_meta_mock.lastModified returns data.csv_s3_LastModified
      csv_s3_meta_mock.getContentStream returns csv_s3_content_stream
      s3Adapter.getMeta(===(data.s3_bucket), ===(data.csv_s3_key)) returns Success(csv_s3_meta_mock)
      s3Adapter.getMeta(===(data.db_s3_key)) returns Failure(mock[Exception])
      db_file_mock.getAbsolutePath returns data.db_temp_file_path
      filesystemAdapter.newTempFile(===(data.db_temp_file_hint)) returns db_file_mock
      filesystemAdapter.newReader(csv_s3_content_stream) returns csv_reader_mock
      liteDbAdapter.create(===(data.db_temp_file_path)) returns db_mock
      csvAdapter.parseReaderAsStream(csv_reader_mock) returns Success(data.csv_stream_no_records)

      handleTask(data.activityParameter) must throwA[RuntimeException]
    }
    "fail task if csv stream has bad records and failOnBadRecord is true" in new WithWorker {
      swfAdapter.config.getOrElse(===("failOnBadRecord"), any[Boolean]) returns true
      csv_s3_meta_mock.lastModified returns data.csv_s3_LastModified
      csv_s3_meta_mock.getContentStream returns csv_s3_content_stream
      s3Adapter.getMeta(===(data.s3_bucket), ===(data.csv_s3_key)) returns Success(csv_s3_meta_mock)
      s3Adapter.getMeta(===(data.db_s3_key)) returns Failure(mock[Exception])
      db_file_mock.getAbsolutePath returns data.db_temp_file_path
      filesystemAdapter.newTempFile(===(data.db_temp_file_hint)) returns db_file_mock
      filesystemAdapter.newReader(csv_s3_content_stream) returns csv_reader_mock
      liteDbAdapter.create(===(data.db_temp_file_path)) returns db_mock
      csvAdapter.parseReaderAsStream(csv_reader_mock) returns Success(data.csv_stream_no_records)

      handleTask(data.activityParameter) must throwA[RuntimeException]
    }
    "complete task by uploading generated db to s3" in new WithWorker {
      csv_s3_meta_mock.lastModified returns data.csv_s3_LastModified
      csv_s3_meta_mock.getContentStream returns csv_s3_content_stream
      s3Adapter.getMeta(===(data.s3_bucket), ===(data.csv_s3_key)) returns Success(csv_s3_meta_mock)
      s3Adapter.getMeta(===(data.db_s3_key)) returns Failure(mock[Exception])
      db_file_mock.getAbsolutePath returns data.db_temp_file_path
      filesystemAdapter.newTempFile(===(data.db_temp_file_hint)) returns db_file_mock
      filesystemAdapter.newReader(csv_s3_content_stream) returns csv_reader_mock
      liteDbAdapter.create(===(data.db_temp_file_path)) returns db_mock
      csvAdapter.parseReaderAsStream(csv_reader_mock) returns Success(data.csv_stream)
      db_mock.batch(anyString) returns db_batch_mock
      filesystemAdapter.gzip(db_file_mock) returns db_gzip_file_mock
      s3Adapter.upload(===(data.db_s3_key), ===(db_gzip_file_mock), anyString, any[Map[String, String]], any[S3Visibility]) returns Success(data.db_s3_uri)

      val result = handleTask(data.activityParameter)

      /* make sure output is complete s3 url */
      result.result must ===(new JsString(data.db_s3_uri.toString))

      there was one(db_mock).execute("create table \"recipients\" (" +
        "\"locationid\" integer, " +
        "\"recipientid\" varchar(100), " +
        "\"email\" varchar(100), " +
        "\"firstname\" varchar(50), " +
        "\"lastname\" varchar(50), " +
        "\"birthday\" date, " +
        "primary key (\"locationid\", \"recipientid\"))")
      there was one(db_mock).batch("insert into \"recipients\" " +
        "(\"recipientid\", \"locationid\", \"email\", \"firstname\", \"lastname\", \"birthday\") " +
        "values (?, ?, ?, ?, ?, ?)")

      there was one(db_batch_mock).param(1, Some("b"))
      there was one(db_batch_mock).param(2, Some(2.toLong))
      there was one(db_batch_mock).param(3, Some("rafael@nike.com"))
      there was one(db_batch_mock).param(4, Some("rafael"))
      there was one(db_batch_mock).param(5, Some("nadal"))
      there was one(db_batch_mock).param(6, Some(new java.sql.Date(sqlDateParser.parse("1986-06-03").getTime)))

      there was one(db_batch_mock).param(1, Some("a"))
      there was one(db_batch_mock).param(2, Some(1.toLong))
      there was one(db_batch_mock).param(3, Some("roger@nike.com"))
      there was one(db_batch_mock).param(4, Some("roger"))
      there was one(db_batch_mock).param(5, Some("federer"))
      there was one(db_batch_mock).param(6, Some(new java.sql.Date(sqlDateParser.parse("1981-08-08").getTime)))

      there was one(db_batch_mock).param(1, Some("c"))
      there was one(db_batch_mock).param(2, Some(3.toLong))
      there was one(db_batch_mock).param(3, Some("novak@uniqlo.com"))
      there was one(db_batch_mock).param(4, Some("novak"))
      there was one(db_batch_mock).param(5, Some("djokovic"))
      there was one(db_batch_mock).param(6, Some(new java.sql.Date(sqlDateParser.parse("1987-05-22").getTime)))

      there was three(db_batch_mock).add()
      there was two(db_batch_mock).execute()

      there was one(db_mock).commit()
      there was one(db_mock).execute("create unique index \"email_unique_idx\" on \"recipients\" (\"email\")")
      there was one(db_mock).execute("create index \"fullname\" on \"recipients\" (\"firstname\", \"lastname\")")
      there was one(db_mock).execute("create index \"bday\" on \"recipients\" (\"birthday\")")

    }
    "complete task by returning cached db uri if db lastModified is equal or greater to csv lastModified" in new WithWorker {
      csv_s3_meta_mock.lastModified returns data.csv_s3_LastModified
      s3Adapter.getMeta(===(data.s3_bucket), ===(data.csv_s3_key)) returns Success(csv_s3_meta_mock)
      db_s3_meta_mock.userMetaData returns data.db_s3_userMetaDataUseCache
      db_s3_meta_mock.s3Uri returns data.db_s3_uri
      s3Adapter.getMeta(===(data.db_s3_key)) returns Success(db_s3_meta_mock)

      val result = handleTask(data.activityParameter)

      /* make sure output is complete s3 url */
      result.result must ===(new JsString(data.db_s3_uri.toString))

      there was no(filesystemAdapter).newTempFileFromStream(any[InputStream], anyString)
    }
  }

  object data {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    val s3_bucket = "balihoo.fulfillment.stuff"
    val csv_s3_key = "my/key/file.csv"
    val param_source = new URI(s"s3://$s3_bucket/$csv_s3_key")
    val param_source_invalid_protocol = new URI(s"http://$s3_bucket/$csv_s3_key")
    val param_dbname = "test.db"
    val param_dtd = new ActivityArgs(Map.empty, Json.obj("columns" -> Json.arr(
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
    )).toString())
    val param_dtd_invalid = new ActivityArgs(Map.empty, Json.obj().toString())
    val activityParameter = new ActivityArgs(Map("source" -> param_source, "dbname" -> param_dbname, "dtd" -> param_dtd))
    val activityParameterWithInvalidDtd = new ActivityArgs(Map("source" -> param_source, "dtd" -> param_dtd_invalid, "dbname" -> param_dbname))
    val activityParameterWithMissingSource = new ActivityArgs(Map("dbname" -> param_dbname, "dtd" -> param_dtd))
    val activityParameterWithUnsupportedProtocol = new ActivityArgs(Map("source" -> param_source_invalid_protocol, "dbname" -> param_dbname, "dtd" -> param_dtd))
    val activityParameterWithMissingDbname = new ActivityArgs(Map("source" -> param_source, "dtd" -> param_dtd))
    val activityParameterWithMissingDtd = new ActivityArgs(Map("source" -> param_source, "dbname" -> param_dbname))
    val headers = List("RECIPIENT", "STORENUM", "emailaddr", "UNUSED", "FNAME", "LNAME", "TYPE", "BDAY")
    val roger = List("a", "1", "roger@nike.com", "some", "roger", "federer", "a", "1981-08-08")
    val rafael = List("b", "2", "rafael@nike.com", "some", "rafael", "nadal", "a", "1986-06-03")
    val novak = List("c", "3", "novak@uniqlo.com", "some", "novak", "djokovic", "a", "1987-05-22")
    val novak_bad = novak.tail
    val csv_stream = headers #:: rafael #:: roger #:: novak #:: Stream.empty
    val csv_stream_no_records = headers #:: Stream.empty
    val csv_stream_bad_record = headers #:: roger #:: novak_bad #:: Stream.empty
    val db_temp_file_path = "/any/path"
    val db_temp_file_hint = param_dbname + ".sqlite"
    val csv_s3_LastModified = new Date()
    val db_s3_LastModified = csv_s3_LastModified // same date means use cache
    val db_s3_userMetaDataUseCache = Map("csvlastmodified" -> dateFormat.format(db_s3_LastModified))
    val db_temp_file_uri = new URI("s3://some/valid")
    val s3_dir = "test_dubdir"
    val db_s3_key = s"$s3_dir/$param_dbname.gz"
    val db_s3_uri = new URI(s"s3://$s3_bucket/$db_s3_key")
  }

  trait WithWorker extends AbstractDatabaseCreate with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent
    with CsvAdapterComponent
    with FilesystemAdapterComponent
    with LightweightDatabaseAdapterComponent {

    override val s3Adapter = mock[AbstractS3Adapter]
    override val csvAdapter = mock[CsvAdapter]
    override val liteDbAdapter = mock[LightweightDatabaseAdapter]
    override val filesystemAdapter = mock[JavaIOFilesystemAdapter]
    override val insertBatchSize = 2
    override val s3dir = data.s3_dir

    val csv_s3_meta_mock = mock[S3Meta]
    val csv_s3_content_stream = mock[S3ObjectInputStream]
    val csv_reader_mock = mock[InputStreamReader]
    val db_s3_meta_mock = mock[S3Meta]
    val db_file_mock = mock[File]
    val db_gzip_file_mock = mock[File]
    val db_mock = mock[LightweightDatabase]
    val db_batch_mock = mock[DbBatch]
  }

  class DbTestContext extends Scope {
    object data {
      val col1 = DatabaseColumnDefinition("someint", "integer", "SOMEINT", Some("primary key"))
      val col2 = DatabaseColumnDefinition("someint2", "integer", "SOMEINT2", Some("primary key"))
      val col3 = DatabaseColumnDefinition("somestring", "varchar(50)", "SOMESTRING", Some("unique"))
      val col4 = DatabaseColumnDefinition("somedate", "date", "SOMEDATE")
      val col5 = DatabaseColumnDefinition("somestring2", "char(3)", "SOMESTRING2", Some("an_index_name"))
      val col6 = DatabaseColumnDefinition("somestring3", "char(3)", "SOMESTRING3", Some("an_index_name"))
      val col7 = DatabaseColumnDefinition("somebool", "boolean", "SOMEBOOL")
      val col8 = DatabaseColumnDefinition("sometimestamp", "timestamp", "SOMETIMESTAMP")
      val col9 = DatabaseColumnDefinition("somereal", "double", "SOMEREAL")
      val tableDefinition =
        DatabaseTableDefinition(
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
