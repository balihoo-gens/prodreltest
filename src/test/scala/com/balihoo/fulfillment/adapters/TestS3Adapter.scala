package com.balihoo.fulfillment.adapters

import java.io.{File, InputStreamReader, InputStream, Reader}
import java.nio.charset.Charset
import java.nio.file.{StandardCopyOption, Files, Path}

import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3ObjectInputStream, S3Object}
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, PropertiesLoader}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class TestS3Adapter extends Specification with Mockito {
  
  "withS3Object" should {
    "invoke aws client and invoke callback function with s3 object" in new TestContext {
      givenS3Object()
      s3Adapter.withS3Object(data.bucket, data.key) { s3Object =>
        s3Object must beAnInstanceOf[S3Object]
      }
    }
    "bubble any client exception" in new TestContext {
      givenClientThrowsException()
      s3Adapter.withS3Object(data.bucket, data.key) { s3Object =>
        failure("code block should not be invoked when there is a failure")
      } must throwA[AmazonClientException]
    }
  }
  "getObjectContentAsReader" should {
    "return a new input stream reader with utf8 as default encoding" in new TestContext {
      givenS3Object()
      givenS3ObjectContent()
      val reader = s3Adapter.getObjectContentAsReader(data.bucket, data.key)
      reader must beAnInstanceOf[InputStreamReader]
      Charset.forName("UTF-8").aliases().contains(reader.getEncoding) must beTrue
    }
    "return a new input stream reader with specified encoding" in new TestContext {
      givenS3Object()
      givenS3ObjectContent()
      val reader = s3Adapter.getObjectContentAsReader(data.bucket, data.key, "Latin1")
      reader must beAnInstanceOf[InputStreamReader]
      Charset.forName("Latin1").aliases().contains(reader.getEncoding) must beTrue
    }
  }
  "getAsTempFile" should {
    "return a copy of the s3 file in a new local temporary file" in new TestContext {
      givenS3Object()
      givenS3ObjectContent()
      val tmpFile = s3Adapter.getAsTempFile(data.bucket, data.key)
      tmpFile must beAnInstanceOf[File]
    }
    "throw an exception if no content copied" in new TestContext {
      givenS3Object()
      givenS3ObjectContent()
      givenNoDataCopied()
      s3Adapter.getAsTempFile(data.bucket, data.key) must throwA[RuntimeException]
    }
  }

  class TestContext extends Scope {

    object data {
      val bucket = "hsgks"
      val key = "jdgfksl/fjdskg/fkjds"
      val s3ObjectMock = mock[S3Object]
      val s3ObjectInputStreamMock = mock[S3ObjectInputStream]
    }

    val DataCopied = 1.toLong
    val NoDataCopied = 0.toLong
    var copyLength = DataCopied /* get around the file copy method override below return value */

    val s3Adapter = new AbstractS3Adapter with SploggerComponent with PropertiesLoaderComponent {
      override val splog = mock[Splogger]
      override lazy val client = mock[AmazonS3Client]
      override val config = mock[PropertiesLoader]
      override def fileCopy(is: InputStream, path: Path) = copyLength
    }

    def givenS3ObjectContent(inputStream: S3ObjectInputStream = data.s3ObjectInputStreamMock) = {
      data.s3ObjectMock.getObjectContent returns inputStream
    }

    def givenS3Object(s3obj: S3Object = data.s3ObjectMock) = {
      s3Adapter.client.getObject(data.bucket, data.key) returns s3obj
    }

    def givenClientThrowsException() = {
      s3Adapter.client.getObject(data.bucket, data.key) throws mock[AmazonClientException]
    }

    def givenNoDataCopied() = copyLength = NoDataCopied

  }

}
