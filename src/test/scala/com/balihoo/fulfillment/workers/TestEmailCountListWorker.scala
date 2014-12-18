package com.balihoo.fulfillment.workers

import java.io.{File, InputStream}

import com.balihoo.fulfillment.adapters._
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

import scala.util.Success

@RunWith(classOf[JUnitRunner])
class TestEmailCountListWorker extends Specification with Mockito {

  "email countlist worker" should {
    "require source parameter" in new WithWorker {
      val in =
        """
          |{
          |  "column": "locationid",
          |  "locations": ["1", "2"],
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"source\"\\]\\)")
    }
    "require source parameter to be an URI" in new WithWorker {
      val in =
        """
          |{
          |  "source": "not valid",
          |  "column": "locationid",
          |  "locations": ["1", "2"],
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("/source string \"not valid\" is not a valid URI")
    }
    "require source parameter to be an URI that starts with s3" in new WithWorker {
      val in =
        """
          |{
          |  "source": "http://some/location",
          |  "column": "locationid",
          |  "locations": ["1", "2"],
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("source parameter protocol is unsupported")
    }
    "require column parameter" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://bucket/key.db",
          |  "locations": ["1", "2"],
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must throwA[ActivitySpecificationException]("object has missing required properties \\(\\[\"column\"\\]\\)")
    }
    "return all non-zero count, for each location specified, ordered by specified column, with query" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://bucket/key.db",
          |  "column": "locationid",
          |  "locations": ["b", "a", "c"],
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin

      val s3StreamMock = mock[InputStream]
      val s3MetaMock = mock[S3Meta]
      val dbFileMock = mock[File]
      val dbMock = mock[LightweightDatabase]

      s3MetaMock.getContentStream returns s3StreamMock
      dbFileMock.getAbsolutePath returns data.dbFilePath
      s3Adapter.getMeta(data.bucket, data.key) returns Success(s3MetaMock)
      filesystemAdapter.newTempFileFromStream(s3StreamMock, data.key) returns dbFileMock
      liteDbAdapter.create(data.dbFilePath) returns dbMock
      dbMock.getAllTableColumns(anyString) returns data.dbColumns
      dbMock.executeAndGetResult(anyString) returns data.dbResults

      invokeAndGetResult(in) must beEqualTo("""[{"locationid":"a","count":3},{"locationid":"c","count":1}]""")

    }
    "return all non-zero count, for each location specified, ordered by specified column, without query" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://bucket/key.db",
          |  "column": "locationid",
          |  "locations": ["b", "a", "c"]
          |}
        """.stripMargin

      val s3StreamMock = mock[InputStream]
      val s3MetaMock = mock[S3Meta]
      val dbFileMock = mock[File]
      val dbMock = mock[LightweightDatabase]

      s3MetaMock.getContentStream returns s3StreamMock
      dbFileMock.getAbsolutePath returns data.dbFilePath
      s3Adapter.getMeta(data.bucket, data.key) returns Success(s3MetaMock)
      filesystemAdapter.newTempFileFromStream(s3StreamMock, data.key) returns dbFileMock
      liteDbAdapter.create(data.dbFilePath) returns dbMock
      dbMock.getAllTableColumns(anyString) returns data.dbColumns
      dbMock.executeAndGetResult(anyString) returns data.dbResults

      invokeAndGetResult(in) must beEqualTo("""[{"locationid":"a","count":3},{"locationid":"c","count":1}]""")

    }
    "throw an exception if database rows are not as expected" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://bucket/key.db",
          |  "column": "locationid",
          |  "locations": ["b", "a", "c"],
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin

      val s3StreamMock = mock[InputStream]
      val s3MetaMock = mock[S3Meta]
      val dbFileMock = mock[File]
      val dbMock = mock[LightweightDatabase]

      s3MetaMock.getContentStream returns s3StreamMock
      dbFileMock.getAbsolutePath returns data.dbFilePath
      s3Adapter.getMeta(data.bucket, data.key) returns Success(s3MetaMock)
      filesystemAdapter.newTempFileFromStream(s3StreamMock, data.key) returns dbFileMock
      liteDbAdapter.create(data.dbFilePath) returns dbMock
      dbMock.getAllTableColumns(anyString) returns data.dbColumns
      dbMock.executeAndGetResult(anyString) returns data.dbBadResults

      invokeAndGetResult(in) must throwA[IllegalStateException]
    }
    "return an empty array if locations is not provided" in new WithWorker {
      val in =
        """
          |{
          |  "source": "s3://bucket/key.db",
          |  "column": "locationid",
          |  "query": {
          |    "select": {
          |      "gender": "m",
          |      "birthday": "$v between '1980-01-01' and '1989-12-31'"
          |    }
          |  }
          |}
        """.stripMargin
      invokeAndGetResult(in) must beEqualTo("[]")
    }
  }

  object data {
    val bucket = "bucket"
    val key = "key.db"
    val dbFilePath = "/a/path"
    val dbColumns = Set("gender", "birthday", "locationid")
    val dbBadResults = Seq(Seq("a"), Seq("b"), Seq("c"))
    val dbResults = Seq(Seq("a", 3), Seq("b", 0), Seq("c", 1))
  }

  trait WithWorker extends AbstractEmailCountListWorker with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent
    with LightweightDatabaseAdapterComponent
    with FilesystemAdapterComponent {

    override val s3Adapter = mock[AbstractS3Adapter]
    override val liteDbAdapter = mock[LightweightDatabaseAdapter]
    override val filesystemAdapter = mock[JavaIOFilesystemAdapter]

    def invokeAndGetResult(in: String) = handleTask(getSpecification.getArgs(in)).serialize()
  }
}
