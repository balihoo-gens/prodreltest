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
class TestEmailFilterListWorker extends Specification with Mockito {

  "AbstractEmailFilterListWorker.handleTask" should {
    "fail task if query param is missing" in new WithWorker {
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

        /* verify everything got cleaned up */
        one(db_mock).close()
        one(csv_outputStream1_mock).close()
        one(csv_outputStream2_mock).close()
        one(csv_outputStream3_mock).close()
        one(csv_file1_mock).delete()
        one(csv_file2_mock).delete()
        one(csv_file3_mock).delete()

        /* verify writes */
        one(csv_writer1_mock).writeRow(data.expectedCsvHeaders)
        one(csv_writer2_mock).writeRow(data.expectedCsvHeaders)
        one(csv_writer3_mock).writeRow(data.expectedCsvHeaders)
        5.times(csv_writer1_mock).writeRow(any[Seq[Any]])
        5.times(csv_writer2_mock).writeRow(any[Seq[Any]])
        three(csv_writer3_mock).writeRow(any[Seq[Any]])

      }

    }
  }

  "QueryDefinition" should {
    "return sql select expression from model" in new WithWorker {
      data.queryDefinition.selectSql must beEqualTo(data.expectedSelectSql)
    }
    "return sql select count expression from model" in new WithWorker {
      data.queryDefinition.selectCountSql must beEqualTo(
        """select count(*) from "recipients" where (("blength"<12) or ("blength">=14 and "blength"<18)) and ("ctype"='COM') and ("fuel"='DIESEL') and (("cstat"='HOLD') or ("cstat"='CUR')) and ("ccexpiredate"='2020-01-01')""")
    }
    "return sql where clause expression from model" in new WithWorker {
      data.queryDefinition.whereExpression must beEqualTo(
        """(("blength"<12) or ("blength">=14 and "blength"<18)) and ("ctype"='COM') and ("fuel"='DIESEL') and (("cstat"='HOLD') or ("cstat"='CUR')) and ("ccexpiredate"='2020-01-01')""")
    }
    "throw exception if sql separator found in clause" in new WithWorker {
      data.queryDefinitionWithSqlInjection.validate() must throwA[RuntimeException]
    }
    "throw exception if sql is empty" in new WithWorker {
      data.queryDefinitionWithEmptySql.validate() must throwA[RuntimeException]
    }
  }
  
  object data {
    val blength = Json.arr("$v<12", "$v>=14 and $v<18")
    val ctype = Json.arr("$v='COM'")
    val fuel = "$v='DIESEL'"
    val cstat = Json.arr("$v='HOLD'", "$v='CUR'")
    val ccexpiredate = "$v='2020-01-01'"
    val param_select = Json.obj(
      "email" -> Json.arr(),
      "firstname" -> "",
      "blength" -> blength,
      "ctype" -> ctype,
      "fuel" -> fuel,
      "cstat" -> cstat,
      "ccexpiredate" -> ccexpiredate)
    val selectWithSqlInjection = Json.obj("somefield" -> "$v is null or 1=1; select * from users")
    val selectWithEmptySql = Json.obj("somefield" -> Json.arr())
    val queryDefinition = QueryDefinition(param_select)
    val queryDefinitionWithSqlInjection = QueryDefinition(selectWithSqlInjection)
    val queryDefinitionWithEmptySql = QueryDefinition(selectWithEmptySql)
    val param_query = Json.obj("select" -> param_select)
    val param_queryInvalid = Json.obj()
    val s3_bucket = "some.bucket"
    val db_s3_key = "long/key/some.db"
    val param_source = s"s3://$s3_bucket/$db_s3_key"
    val param_source_invalid = "://"
    val param_source_invalid_protocol = "http://some/uri"
    val param_pageSize = 4
    val activityParameter = ActivityParameters("source" -> param_source, "query" -> param_query, "pageSize" -> param_pageSize)
    val activityParameterQueryMissing = ActivityParameters("source" -> param_source, "pageSize" -> param_pageSize)
    val activityParameterQueryInvalid = ActivityParameters("source" -> param_source, "query" -> param_queryInvalid, "pageSize" -> param_pageSize)
    val activityParameterSourceMissing = ActivityParameters("query" -> param_query, "pageSize" -> param_pageSize)
    val activityParameterSourceInvalidProtocol = ActivityParameters("source" -> param_source_invalid_protocol, "query" -> param_query, "pageSize" -> param_pageSize)
    val activityParameterSourceInvalid = ActivityParameters("source" -> param_source_invalid, "query" -> param_query, "pageSize" -> param_pageSize)
    val db_s3_meta_filename = db_s3_key.split("/").last
    val db_file_path = "some/file"
    val csv_s3_key1 = s"mock/$db_s3_meta_filename.1.csv"
    val csv_s3_key2 = s"mock/$db_s3_meta_filename.2.csv"
    val csv_s3_key3 = s"mock/$db_s3_meta_filename.3.csv"
    val csv_s3_uri1 = new URI("s3://host1/path1")
    val csv_s3_uri2 = new URI("s3://host2/path2")
    val csv_s3_uri3 = new URI("s3://host3/path3")
    val selectSql = queryDefinition.selectSql
    val recordsCount = 10
    val pageSizeInvalid = 0
    val expectedCsvHeaders = Seq("email", "firstname", "blength", "ctype", "fuel", "cstat", "ccexpiredate")
    val expectedSelectSql = """select "email", "firstname", "blength", "ctype", "fuel", "cstat", "ccexpiredate" from "recipients" where (("blength"<12) or ("blength">=14 and "blength"<18)) and ("ctype"='COM') and ("fuel"='DIESEL') and (("cstat"='HOLD') or ("cstat"='CUR')) and ("ccexpiredate"='2020-01-01') order by "email""""
    val expectedPageCount = recordsCount / param_pageSize + 1
  }

  trait WithWorker extends AbstractEmailFilterListWorker with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent
    with LightweightDatabaseAdapterComponent
    with FilesystemAdapterComponent
    with CsvAdapterComponent {

    /* overrides */
    override val s3Adapter = mock[AbstractS3Adapter]
    override val liteDbAdapter = mock[LightweightDatabaseAdapter]
    override val csvAdapter = mock[CsvAdapter]
    override val filesystemAdapter = mock[JavaIOFilesystemAdapter]

    /* hack into completeTask base worker to get result */
    var test_complete_result = ""
    override def completeTask(result: String) = {
      test_complete_result = result
    }

    /* mocks */
    val db_s3_meta_mock = mock[S3Meta]
    val db_s3_contentStream_mock = mock[S3ObjectInputStream]
    val db_file_mock = mock[File]
    val db_mock = mock[LightweightDatabase]
    val db_paged_resultSet_mock = mock[DbPagedResultSet]
    val db_resultset_page_mock = mock[DbResultSetPage]

  }
}
