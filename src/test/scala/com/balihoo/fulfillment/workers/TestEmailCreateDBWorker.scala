package com.balihoo.fulfillment.workers

import java.io.{InputStreamReader, File}
import java.net.URISyntaxException

import com.amazonaws.services.s3.model.S3Object
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

  "getSpecification" should {
    "return a valid specification" in new TestContext {
      val spec = worker.getSpecification
      spec must beAnInstanceOf[ActivitySpecification]
      spec.params.size must beEqualTo(3)
      spec.params.count(_.name == "source") must beEqualTo(1)
      spec.params.count(_.name == "dbname") must beEqualTo(1)
      spec.params.count(_.name == "dtd") must beEqualTo(1)
      spec.result must beAnInstanceOf[ActivityResult]
      spec.result.rtype must beEqualTo("string")
      spec.result.sensitive must beFalse
      spec.description must not(beEmpty)
    }
  }

  "handleTask" should {
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
      givenLiteDb()
      val activityParameter = new ActivityParameters(Map(
        "source" -> data.source,
        "dbname" -> data.dbname,
        "dtd" -> data.dtd))

      worker.handleTask(activityParameter)

      there was one(worker.dbMock).execute("create table recipients (locationid integer, recipientid varchar(100), email varchar(100), firstname varchar(50), lastname varchar(50), birthday date)")
      there was one(worker.dbMock).batch("insert into recipients (recipientid, locationid, email, firstname, lastname, birthday) values (?, ?, ?, ?, ?, ?)")

      there was one(worker.dbBatchMock).param(1, "b")
      there was one(worker.dbBatchMock).param(2, 2)
      there was one(worker.dbBatchMock).param(3, "rafael@nike.com")
      there was one(worker.dbBatchMock).param(4, "rafael")
      there was one(worker.dbBatchMock).param(5, "nadal")
      there was one(worker.dbBatchMock).param(6, "1986-06-03")

      there was one(worker.dbBatchMock).param(1, "a")
      there was one(worker.dbBatchMock).param(2, 1)
      there was one(worker.dbBatchMock).param(3, "roger@nike.com")
      there was one(worker.dbBatchMock).param(4, "roger")
      there was one(worker.dbBatchMock).param(5, "federer")
      there was one(worker.dbBatchMock).param(6, "1981-08-08")

      there was one(worker.dbBatchMock).param(1, "c")
      there was one(worker.dbBatchMock).param(2, 3)
      there was one(worker.dbBatchMock).param(3, "novak@uniqlo.com")
      there was one(worker.dbBatchMock).param(4, "novak")
      there was one(worker.dbBatchMock).param(5, "djokovic")
      there was one(worker.dbBatchMock).param(6, "1987-05-22")

      there was three(worker.dbBatchMock).add()
      there was two(worker.dbBatchMock).execute()

      there was one(worker.dbMock).commit()
      there was one(worker.dbMock).close()
      there was one(worker.readerMock).close()
      there was one(worker.s3Adapter).putPublic(===("mock"), beMatching(s"mock/\\d+/${data.dbname}"), ===(worker.dbFileMock))
      there was one(worker.dbMock).destroy()

      /* make sure output is complete s3 url */
      worker.test_complete_result must beMatching(s"s3://mock/mock/\\d+/" + data.dbname)
    }

  }

  class TestContext extends Scope {

    object data {
      val s3bucket = "balihoo.fulfillment.stuff"
      val s3key = "my/key/file.csv"
      val source = s"s3://$s3bucket/$s3key"
      val sourceWithInvalidScheme = s"http://$s3bucket/$s3key"
      val dbname = "test.db"
      val dtdJson = Json.obj("columns" -> Json.arr(
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
          "source" -> "EMAILADDR"
        ),
        Json.obj(
          "name" -> "firstName",
          "type" -> "varchar(50)",
          "source" -> "FNAME"
        ),
        Json.obj(
          "name" -> "lastName",
          "type" -> "varchar(50)",
          "source" -> "LNAME"
        ),
        Json.obj(
          "name" -> "birthday",
          "type" -> "date",
          "source" -> "BDAY",
          "index" -> "bday"
        )
      ))
      val dtd = Json.stringify(dtdJson)
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
      with LightweightDatabaseAdapterComponent {

      trait TestDbType extends LightweightDatabase with LightweightFileDatabase
      override type DB_TYPE = TestDbType
      override val s3Adapter = mock[AbstractS3Adapter]
      override val csvAdapter = mock[CsvAdapter]
      override val liteDbAdapter = mock[LightweightDatabaseAdapter]
      val readerMock = mock[InputStreamReader]
      val dbMock = mock[DB_TYPE]
      val dbFileMock = mock[File]
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
      worker.dbMock.file returns worker.dbFileMock
      worker.dbMock.batch(anyString) returns worker.dbBatchMock
      worker.liteDbAdapter.create(===(data.dbname)) returns worker.dbMock
    }

  }

}
