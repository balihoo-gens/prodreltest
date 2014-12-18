package com.balihoo.fulfillment.workers

import java.io.{File, OutputStream}
import java.net.URI

import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.workers.ses.AbstractEmailFilterListWorker
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

import scala.collection.immutable.TreeSet
import scala.util.Success

@RunWith(classOf[JUnitRunner])
class TestEmailFilterListWorker extends Specification with Mockito {

  "email filter list worker" should {
    "fail task if query param is missing" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://some/file",
          |  "pageSize": 1
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"query\"\\]\\)")
    }
    "fail task if query param is invalid json" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://some/file",
          |  "pageSize": 1,
          |  "query": {
          |   "invalid": {}
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("/query object has missing required properties \\(\\[\"select\"\\]\\)")
    }
    "fail task if source param is missing" in new WithWorker {
      val in =
        """
          |{
          |  "pageSize": 1,
          |  "query": {
          |    "select": {
          |      "email":        [],
          |      "firstname":    [],
          |      "blength":      [ "$v<12", "$v>=14 and $v<18" ],
          |      "ctype":        [ "$v='COM'" ],
          |      "fuel":         [ "$v='DIESEL'" ],
          |      "cstat":        [ "$v='HOLD'", "$v='CUR'" ],
          |      "ccexpiredate": [ "$v='2020-01-01'" ]
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"source\"\\]\\)")
    }
    "fail task if source param is invalid uri" in new WithWorker {
      val in =
        """
          |{
          |  "source": "in valid",
          |  "pageSize": 1,
          |  "query": {
          |    "select": {
          |      "email":        [],
          |      "firstname":    [],
          |      "blength":      [ "$v<12", "$v>=14 and $v<18" ],
          |      "ctype":        [ "$v='COM'" ],
          |      "fuel":         [ "$v='DIESEL'" ],
          |      "cstat":        [ "$v='HOLD'", "$v='CUR'" ],
          |      "ccexpiredate": [ "$v='2020-01-01'" ]
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("/source string \"in valid\" is not a valid URI")
    }
    "fail task if source param is invalid uri protocol" in new WithWorker {
      val in =
        """
          |{
          |  "source": "http://invalid/right/now",
          |  "pageSize": 1,
          |  "query": {
          |    "select": {
          |      "email":        [],
          |      "firstname":    [],
          |      "blength":      [ "$v<12", "$v>=14 and $v<18" ],
          |      "ctype":        [ "$v='COM'" ],
          |      "fuel":         [ "$v='DIESEL'" ],
          |      "cstat":        [ "$v='HOLD'", "$v='CUR'" ],
          |      "ccexpiredate": [ "$v='2020-01-01'" ]
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("Invalid source protocol")
    }
    "fail task if pageSize param is missing" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://invalid/right/now",
          |  "query": {
          |    "select": {
          |      "email":        [],
          |      "firstname":    [],
          |      "blength":      [ "$v<12", "$v>=14 and $v<18" ],
          |      "ctype":        [ "$v='COM'" ],
          |      "fuel":         [ "$v='DIESEL'" ],
          |      "cstat":        [ "$v='HOLD'", "$v='CUR'" ],
          |      "ccexpiredate": [ "$v='2020-01-01'" ]
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"pageSize\"\\]\\)")
    }
    "fail task if pageSize param is invalid" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://invalid/right/now",
          |  "pageSize": "invalid",
          |  "query": {
          |    "select": {
          |      "email":        [],
          |      "firstname":    [],
          |      "blength":      [ "$v<12", "$v>=14 and $v<18" ],
          |      "ctype":        [ "$v='COM'" ],
          |      "fuel":         [ "$v='DIESEL'" ],
          |      "cstat":        [ "$v='HOLD'", "$v='CUR'" ],
          |      "ccexpiredate": [ "$v='2020-01-01'" ]
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("/pageSize instance type \\(string\\) does not match any allowed primitive type \\(allowed: \\[\"integer\"\\]\\)")
    }
    "complete task if db file downloaded from s3, query performed, csv files created and uploaded to s3" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://some.bucket/long/key/some.db.gz",
          |  "pageSize": 4,
          |  "query": {
          |    "select": {
          |      "email":        [],
          |      "firstname":    [],
          |      "blength":      [ "$v<12", "$v>=14 and $v<18" ],
          |      "ctype":        [ "$v='COM'" ],
          |      "fuel":         [ "$v='DIESEL'" ],
          |      "cstat":        [ "$v='HOLD'", "$v='CUR'" ],
          |      "ccexpiredate": [ "$v='2020-01-01'" ]
          |    }
          |  }
          |}
        """.stripMargin
      s3Adapter.getMeta(===(data.s3_bucket), ===(data.db_s3_key)) returns Success(db_s3_meta_mock)
      db_s3_meta_mock.filename returns data.db_s3_meta_filename
      db_s3_meta_mock.filenameNoExtension returns data.db_s3_meta_filename
      db_s3_meta_mock.getContentStream returns db_s3_contentStream_mock
      db_file_mock.getAbsolutePath returns data.db_file_path
      filesystemAdapter.newTempFileFromStream(db_s3_contentStream_mock, data.db_s3_key) returns db_file_mock
      liteDbAdapter.create(===(data.db_file_path)) returns db_mock
      val page1 = Seq(mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart)
      val page2 = Seq(mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart, mock[Seq[Any]].smart)
      val page3 = Seq(mock[Seq[Any]].smart, mock[Seq[Any]].smart)
      val pages = Seq(page1, page2, page3)
      db_mock.selectCount(s"""select count(*) from "recipients"""") returns 100
      db_mock.selectCount(s"""select count(*) from "recipients" where ${data.selectWhere}""") returns data.recordsCount
      liteDbAdapter.calculatePageCount(data.recordsCount, data.pageSize) returns data.pageCount
      db_mock.getAllTableColumns(===(data.dbTableName)) returns data.dbColumns
      db_mock.pagedSelect(===(s"""select ${data.selectColumns} from "recipients" where ${data.selectWhere} order by "blength""""), ===(data.recordsCount), ===(data.pageSize)) returns pages

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
      filesystemAdapter.gzip(csv_file1_mock) returns csv_file1_mock
      filesystemAdapter.gzip(csv_file2_mock) returns csv_file2_mock
      filesystemAdapter.gzip(csv_file3_mock) returns csv_file3_mock
      s3Adapter.upload(data.csv_s3_key1, csv_file1_mock) returns Success(data.csv_s3_uri1)
      s3Adapter.upload(data.csv_s3_key2, csv_file2_mock) returns Success(data.csv_s3_uri2)
      s3Adapter.upload(data.csv_s3_key3, csv_file3_mock) returns Success(data.csv_s3_uri3)

      invokeAndGetResult(in) must beEqualTo(s"""["${data.csv_s3_uri1}","${data.csv_s3_uri2}","${data.csv_s3_uri3}"]""")

      got {

        /* verify writes */
        one(csv_writer1_mock).writeRow(data.csvHeaders)
        one(csv_writer2_mock).writeRow(data.csvHeaders)
        one(csv_writer3_mock).writeRow(data.csvHeaders)
        5.times(csv_writer1_mock).writeRow(any[Seq[Any]])
        5.times(csv_writer2_mock).writeRow(any[Seq[Any]])
        three(csv_writer3_mock).writeRow(any[Seq[Any]])

      }
    }
  }

  object data {
    val s3_bucket = "some.bucket"
    val db_s3_key = "long/key/some.db.gz"
    val db_s3_meta_filename = db_s3_key.split("/").last
    val db_s3_meta_filename_noext = db_s3_key.split("/").last.split('.').init.mkString(".")
    val db_file_path = "some/file"
    val dbTableName = "recipients"
    val dbColumns = TreeSet("blength", "ccexpiredate", "cstat", "ctype", "email", "firstname", "fuel")
    val selectColumns = """"blength", "ccexpiredate", "cstat", "ctype", "email", "firstname", "fuel""""
    val selectWhere = """(("blength"<12) or ("blength">=14 and "blength"<18)) and ("ccexpiredate"='2020-01-01') and (("cstat"='HOLD') or ("cstat"='CUR')) and ("ctype"='COM') and ("fuel"='DIESEL')"""
    val csvHeaders = dbColumns.toSeq
    val csv_s3_key1 = s"mock/$db_s3_meta_filename.1.csv.gz"
    val csv_s3_key2 = s"mock/$db_s3_meta_filename.2.csv.gz"
    val csv_s3_key3 = s"mock/$db_s3_meta_filename.3.csv.gz"
    val csv_s3_uri1 = new URI("s3://host1/path1")
    val csv_s3_uri2 = new URI("s3://host2/path2")
    val csv_s3_uri3 = new URI("s3://host3/path3")
    val recordsCount = 10
    val pageSize = 4
    val pageCount = 3
    val pageSizeInvalid = 0
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

    def invokeAndGetResult(in: String) = handleTask(getSpecification.getArgs(in)).serialize()

    /* mocks */
    val db_s3_meta_mock = mock[S3Meta]
    val db_s3_contentStream_mock = mock[S3ObjectInputStream]
    val db_file_mock = mock[File]
    val db_mock = mock[LightweightDatabase]
    val db_paged_resultSet_mock = mock[DbPagedResultSet]
    val db_resultset_page_mock = mock[DbResultSetPage]

  }
}
