package com.balihoo.fulfillment.workers

import java.io.{File, OutputStream}
import java.net.URI

import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.balihoo.fulfillment.adapters._
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope
import play.api.libs.json.{JsArray, JsString, Json}

import scala.util.{Success, Try}

@RunWith(classOf[JUnitRunner])
class TestUrlToS3Saver extends Specification with Mockito {

  "AbstractUrlToS3Saver.handleTask" should {
    data.paramsets.foreach { key, value =>
      "fail task if url param is missing" in new WithWorker {
        fail
      }
    }
    /*
    "fail task if url param is missing" in new WithWorker {
      handleTask(data.activityParameterQueryMissing) must throwA[IllegalArgumentException]
    }
    "fail task if query param is invalid json" in new WithWorker {
      handleTask(data.activityParameterQueryInvalid) must throwA[IllegalArgumentException]
    }
    "fail task if source param is missing" in new WithWorker {
      handleTask(data.activityParameterSourceMissing) must throwA[IllegalArgumentException]
    }
    "fail task if source param is invalid uri" in new WithWorker {
      handleTask(data.activityParameterSourceInvalid) must throwA[IllegalArgumentException]
    }
    "fail task if source param is invalid uri protocol" in new WithWorker {
      Try(handleTask(data.activityParameterSourceInvalidProtocol)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if pageSize param is missing" in new WithWorker {
      val params = Map("query" -> data.param_query, "source" -> data.param_source)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if pageSize param is invalid" in new WithWorker {
      val params = Map("query" -> data.param_query, "source" -> data.param_source, "pageSize" -> data.pageSizeInvalid)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "complete task if db file downloaded from s3, query performed, csv files created and uploaded to s3" in new WithWorker {

      // setup
      s3Adapter.getMeta(===(data.s3_bucket), ===(data.db_s3_key)) returns Success(db_s3_meta_mock)
      db_s3_meta_mock.filename returns data.db_s3_meta_filename
      db_s3_meta_mock.getContentStream returns db_s3_contentStream_mock
      db_file_mock.getAbsolutePath returns data.db_file_path
      filesystemAdapter.newTempFileFromStream(db_s3_contentStream_mock, data.db_s3_key) returns db_file_mock
      liteDbAdapter.create(===(data.db_file_path)) returns db_mock
      liteDbAdapter.calculatePageCount(===(data.recordsCount), ===(data.param_pageSize)) returns data.expectedPageCount
      db_mock.selectCount(===(data.queryDefinition.selectCountSql)) returns data.recordsCount

      val (csv_file1_mock, csv_file2_mock, csv_file3_mock) = (mock[File], mock[File], mock[File])
      val (csv_outputStream1_mock, csv_outputStream2_mock, csv_outputStream3_mock) = (mock[OutputStream], mock[OutputStream], mock[OutputStream])
      val (csv_writer1_mock, csv_writer2_mock, csv_writer3_mock) = (mock[CsvWriter], mock[CsvWriter], mock[CsvWriter])
      filesystemAdapter.newOutputStream(csv_file1_mock) returns csv_outputStream1_mock
      filesystemAdapter.newOutputStream(csv_file2_mock) returns csv_outputStream2_mock
      filesystemAdapter.newOutputStream(csv_file3_mock) returns csv_outputStream3_mock
      filesystemAdapter.newTempFile(data.csv_s3_key1) returns csv_file1_mock
      filesystemAdapter.newTempFile(data.csv_s3_key2) returns csv_file2_mock
      filesystemAdapter.newTempFile(data.csv_s3_key3) returns csv_file3_mock
      csvAdapter.newWriter(csv_outputStream1_mock) returns csv_writer1_mock
      csvAdapter.newWriter(csv_outputStream2_mock) returns csv_writer2_mock
      csvAdapter.newWriter(csv_outputStream3_mock) returns csv_writer3_mock
      s3Adapter.upload(data.csv_s3_key1, csv_file1_mock) returns Success(data.csv_s3_uri1)
      s3Adapter.upload(data.csv_s3_key2, csv_file2_mock) returns Success(data.csv_s3_uri2)
      s3Adapter.upload(data.csv_s3_key3, csv_file3_mock) returns Success(data.csv_s3_uri3)

      val page1 = Seq(mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart)
      val page2 = Seq(mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart)
      val page3 = Seq(mock[Seq[Any]].smart, mock[Seq[Any]].smart)
      val pages = Seq(page1, page2, page3)
      db_mock.pagedSelect(===(data.selectSql), ===(data.recordsCount), ===(data.param_pageSize)) returns pages

      // invocation
      handleTask(data.activityParameter)

      // verifications
      val expectedResult = Seq(data.csv_s3_uri1, data.csv_s3_uri2, data.csv_s3_uri3).map(_.toString)
      test_complete_result must beEqualTo(JsArray(expectedResult.map(JsString)).toString())

      got {

        // verify everything got cleaned up
        one(db_mock).close()
        one(csv_outputStream1_mock).close()
        one(csv_outputStream2_mock).close()
        one(csv_outputStream3_mock).close()
        one(csv_file1_mock).delete()
        one(csv_file2_mock).delete()
        one(csv_file3_mock).delete()

        // verify writes
        one(csv_writer1_mock).writeRow(data.expectedCsvHeaders)
        one(csv_writer2_mock).writeRow(data.expectedCsvHeaders)
        one(csv_writer3_mock).writeRow(data.expectedCsvHeaders)
        5.times(csv_writer1_mock).writeRow(any[Seq[Any]])
        5.times(csv_writer2_mock).writeRow(any[Seq[Any]])
        three(csv_writer3_mock).writeRow(any[Seq[Any]])

      }
*/
    }
  }

  object data {
    val param_query = Json.obj("select" -> param_select)
    val param_queryInvalid = Json.obj()
    val s3_bucket = "some.bucket"
    val db_s3_key = "long/key/some.db"
    val param_source = s"s3://$s3_bucket/$db_s3_key"
    val param_source_invalid = "://"
    val valid_uri = "http://some/uri"
    val base_params = ActivityParameters(
      "source" -> valid_uri,
      "method" -> "POST",
      "target" -> "some_file.name",
      "headers" -> valid_headers,
      "body" -> valid_body
    )
 
    val activityParameter = ActivityParameters("source" -> param_source, "query" -> param_query, "pageSize" -> param_pageSize)
    val activityParameterQueryMissing = ActivityParameters("source" -> param_source, "pageSize" -> param_pageSize)
    val activityParameterQueryInvalid = ActivityParameters("source" -> param_source, "query" -> param_queryInvalid, "pageSize" -> param_pageSize)
    val activityParameterSourceMissing = ActivityParameters("query" -> param_query, "pageSize" -> param_pageSize)
    val activityParameterSourceInvalidProtocol = ActivityParameters("source" -> param_source_invalid_protocol, "query" -> param_query, "pageSize" -> param_pageSize)
    val activityParameterSourceInvalid = ActivityParameters("source" -> param_source_invalid, "query" -> param_query, "pageSize" -> param_pageSize)
  }

  trait WithWorker extends AbstractUrlToS3Saver with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent {

    /* overrides */
    override val s3Adapter = mock[AbstractS3Adapter]

    /* hack into completeTask base worker to get result */
    var test_complete_result = ""
    override def completeTask(result: String) = {
      test_complete_result = result
    }

    /* mocks */
    val db_s3_meta_mock = mock[S3Meta]
    val db_s3_contentStream_mock = mock[S3ObjectInputStream]
  }
}
