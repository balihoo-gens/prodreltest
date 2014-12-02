package com.balihoo.fulfillment.workers

import java.io.{FileOutputStream, File}

import com.balihoo.fulfillment.adapters._
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope
import play.api.libs.json.{JsArray, JsString, Json}

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class TestEmailFilterListWorker extends Specification with Mockito {

  "AbstractEmailFilterListWorker.handleTask" should {
    "fail task if query param is missing" in new WithWorker {
      val params = Map("source" -> data.source, "pageSize" -> data.pageSize)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if query param is invalid json" in new WithWorker {
      val params = Map("source" -> data.source, "query" -> data.queryInvalid, "pageSize" -> data.pageSize)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if source param is missing" in new WithWorker {
      val params = Map("query" -> data.query, "pageSize" -> data.pageSize)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if source param is invalid uri" in new WithWorker {
      val params = Map("source" -> data.sourceInvalid, "query" -> data.query, "pageSize" -> data.pageSize)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if source param is invalid uri protocol" in new WithWorker {
      val params = Map("source" -> data.sourceInvalidProtocol, "query" -> data.query, "pageSize" -> data.pageSize)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if pageSize param is missing" in new WithWorker {
      val params = Map("query" -> data.query, "source" -> data.source)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail task if pageSize param is invalid" in new WithWorker {
      val params = Map("query" -> data.query, "source" -> data.source, "pageSize" -> data.pageSizeInvalid)
      val activityParameter = new ActivityParameters(params)
      Try(handleTask(activityParameter)) must beFailedTry.withThrowable[IllegalArgumentException]
    }
    "complete task if db file downloaded from s3, query performed, csv files created and uploaded to s3" in new WithWorker {

      // setup
      s3Adapter.getAsTempFile(===(data.s3bucket), ===(data.s3key), ===(None)) returns dbFileMock
      liteDbAdapter.create(dbFileMock) returns dbMock
      liteDbAdapter.calculatePageCount(===(data.recordsCount), ===(data.pageSize)) returns data.expectedPageCount
      dbMock.selectCount(===(data.queryDefinition.selectCountSql)) returns data.recordsCount

      val (fosMock1, fosMock2, fosMock3) = (mock[FileOutputStream], mock[FileOutputStream], mock[FileOutputStream])
      val (fileMock1, fileMock2, fileMock3) = (mock[File], mock[File], mock[File])
      val (csvWriterMock1, csvWriterMock2, csvWriterMock3) = (mock[CsvWriter], mock[CsvWriter], mock[CsvWriter])
      val ffos1 = fileMock1 -> fosMock1
      val ffos2 = fileMock2 -> fosMock2
      val ffos3 = fileMock3 -> fosMock3

      filesystemAdapter.newTempFileOutputStream(===("some.db.1"), ===(".csv")) returns ffos1
      filesystemAdapter.newTempFileOutputStream(===("some.db.2"), ===(".csv")) returns ffos2
      filesystemAdapter.newTempFileOutputStream(===("some.db.3"), ===(".csv")) returns ffos3
      csvAdapter.newWriter(fosMock1) returns csvWriterMock1
      csvAdapter.newWriter(fosMock2) returns csvWriterMock2
      csvAdapter.newWriter(fosMock3) returns csvWriterMock3

      page1.hasNext returns true thenReturns true thenReturns true thenReturns true thenReturns false
      page1.next returns mock[Seq[Any]].smart
      page2.hasNext returns true thenReturns true thenReturns true thenReturns true thenReturns false
      page2.next returns mock[Seq[Any]].smart
      page3.hasNext returns true thenReturns true thenReturns false
      page3.next returns mock[Seq[Any]].smart
      dbPagedResultSetMock.hasNext returns true thenReturns true thenReturns true thenReturns false
      dbPagedResultSetMock.next returns page1 thenReturns page2 thenReturns page3
      dbMock.pagedSelect(===(data.selectSql), ===(data.recordsCount), ===(data.pageSize)) returns dbPagedResultSetMock

      // invocation
      val activityParameter = new ActivityParameters(Map("source" -> data.source, "query" -> data.query, "pageSize" -> data.pageSize))
      handleTask(activityParameter)

      // verifications
      val expectedResult = Seq("s3://mock/mock/some.db.1.csv", "s3://mock/mock/some.db.2.csv", "s3://mock/mock/some.db.3.csv")
      test_complete_result must beEqualTo(JsArray(expectedResult.map(JsString)).toString())

      got {

        /* verify db interaction */
        one(liteDbAdapter).create(dbFileMock)
        one(liteDbAdapter).calculatePageCount(data.recordsCount, data.pageSize)
        one(dbMock).selectCount(s"""select count(*) from "${data.queryDefinition.getTableName}"""")
        one(dbMock).selectCount(data.queryDefinition.selectCountSql)
        one(dbMock).pagedSelect(data.queryDefinition.selectSql, data.recordsCount, data.pageSize)
        one(dbMock).close()

        /* verify tmp s3 file interaction */
        one(dbFileMock).getAbsolutePath
        one(dbFileMock).delete()

        /* verify temp csv files interaction */
        two(fosMock1).close()
        one(fileMock1).getAbsolutePath
        one(fileMock1).delete()
        two(fosMock2).close()
        one(fileMock2).getAbsolutePath
        one(fileMock2).delete()
        two(fosMock3).close()
        one(fileMock3).getAbsolutePath
        one(fileMock3).delete()

        /* verify csv writer interaction */
        one(csvAdapter).newWriter(fosMock1)
        one(csvAdapter).newWriter(fosMock2)
        one(csvAdapter).newWriter(fosMock3)
        val expectedCsvHeaders = Seq("email", "firstname", "blength", "ctype", "fuel", "cstat", "ccexpiredate")
        one(csvWriterMock1).writeRow(===(expectedCsvHeaders))
        5.times(csvWriterMock1).writeRow(any[Seq[Any]])
        one(csvWriterMock2).writeRow(===(expectedCsvHeaders))
        5.times(csvWriterMock2).writeRow(any[Seq[Any]])
        one(csvWriterMock3).writeRow(===(expectedCsvHeaders))
        three(csvWriterMock3).writeRow(any[Seq[Any]])



        /* verify s3 interactions */
        one(s3Adapter).getAsTempFile(data.s3bucket, data.s3key, None)
        one(s3Adapter).putPublic("mock", "mock/some.db.1.csv", fileMock1)
        one(s3Adapter).putPublic("mock", "mock/some.db.2.csv", fileMock2)
        one(s3Adapter).putPublic("mock", "mock/some.db.3.csv", fileMock3)

        /* verify all interactions expected */
        noMoreCallsTo(dbFileMock, dbMock, liteDbAdapter, csvAdapter, s3Adapter)
        noMoreCallsTo(fosMock1, fosMock2, fosMock3)
        noMoreCallsTo(fileMock1, fileMock2, fileMock3)
        noMoreCallsTo(csvWriterMock1, csvWriterMock2, csvWriterMock3)

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
      Try(data.queryDefinitionWithSqlInjection.validate()) must beAFailedTry.withThrowable[RuntimeException]
    }
    "throw exception if sql is empty" in new WithWorker {
      Try(data.queryDefinitionWithEmptySql.validate()) must beAFailedTry.withThrowable[RuntimeException]
    }
  }
  
  object data {
    val blength = Json.arr("$v<12", "$v>=14 and $v<18")
    val ctype = Json.arr("$v='COM'")
    val fuel = "$v='DIESEL'"
    val cstat = Json.arr("$v='HOLD'", "$v='CUR'")
    val ccexpiredate = "$v='2020-01-01'"
    val select = Json.obj(
      "email" -> Json.arr(),
      "firstname" -> "",
      "blength" -> blength,
      "ctype" -> ctype,
      "fuel" -> fuel,
      "cstat" -> cstat,
      "ccexpiredate" -> ccexpiredate)
    val selectWithSqlInjection = Json.obj("somefield" -> "$v is null or 1=1; select * from users")
    val selectWithEmptySql = Json.obj("somefield" -> Json.arr())
    val queryDefinition = QueryDefinition(select)
    val queryDefinitionWithSqlInjection = QueryDefinition(selectWithSqlInjection)
    val queryDefinitionWithEmptySql = QueryDefinition(selectWithEmptySql)
    val query = Json.obj("select" -> select)
    val queryInvalid = Json.obj()
    val s3bucket = "some.bucket"
    val s3key = "/long/key/some.db"
    val source = s"s3://$s3bucket/$s3key"
    val sourceInvalid = "://"
    val sourceInvalidProtocol = "http://some/uri"
    val selectSql = queryDefinition.selectSql
    val recordsCount = 10
    val pageSize = 4
    val pageSizeInvalid = 0
    val expectedSelectSql = """select "email", "firstname", "blength", "ctype", "fuel", "cstat", "ccexpiredate" from "recipients" where (("blength"<12) or ("blength">=14 and "blength"<18)) and ("ctype"='COM') and ("fuel"='DIESEL') and (("cstat"='HOLD') or ("cstat"='CUR')) and ("ccexpiredate"='2020-01-01') order by "email""""
    val expectedPageCount = recordsCount / pageSize + 1
  }

  class WithWorker extends AbstractEmailFilterListWorker
    with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent
    with LightweightDatabaseAdapterComponent
    with CsvAdapterComponent
    with FilesystemAdapterComponent {    

    /* overrides */
    override val s3Adapter = mock[AbstractS3Adapter]
    override val liteDbAdapter = mock[LightweightDatabaseAdapter]
    override val csvAdapter = mock[CsvAdapter]
    override val filesystemAdapter = mock[FilesystemAdapter]

    /* hack into completeTask base worker to get result */
    var test_complete_result = ""
    override def completeTask(result: String) = {
      test_complete_result = result
    }

    /* mocks */
    val dbFileMock = mock[File]
    val dbMock = mock[LightweightDatabase with LightweightFileDatabase]
    val dbPagedResultSetMock = mock[DbPagedResultSet]
    val dbResultSetPageMock = mock[DbResultSetPage]
    val page1 = mock[DbResultSetPage]
    val page2 = mock[DbResultSetPage]
    val page3 = mock[DbResultSetPage]

  }
}
