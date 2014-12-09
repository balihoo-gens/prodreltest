package com.balihoo.fulfillment.adapters

import java.io.File
import java.net.URI

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{PutObjectRequest, PutObjectResult, S3Object}
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.balihoo.fulfillment.config.{PropertiesLoader, PropertiesLoaderComponent}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import org.apache.http.client.methods.HttpRequestBase
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

@RunWith(classOf[JUnitRunner])
class TestS3Adapter extends Specification with Mockito {

  "get" should {
    "return a success to an S3Meta object if aws client request succeed" in new WithAdapter {
      val awsObjectMock = mock[S3Object]
      client.getObject(data.bucket, data.key) returns awsObjectMock

      val result = getMeta(data.bucket, data.key)

      result must beSuccessfulTry[S3Meta].withValue(S3Meta(awsObjectMock, data.key, data.bucket))
    }
    "return a failure if aws client request failed" in new WithAdapter {
      client.getObject(data.bucket, data.key) throws new AmazonServiceException("aws is screwed")

      val result = getMeta(data.bucket, data.key)

      result must beFailedTry.withThrowable[AmazonServiceException]
    }
  }
  "getObjectContentAsString" should {
    "return a string" in new TestContext {
      val expected = "I expect perfection!"
      val inputStream = spy(new S3ObjectInputStream(new ByteArrayInputStream(expected.getBytes), mock[HttpRequestBase]))
      givenS3Object()
      givenS3ObjectContent(inputStream)
      val result = s3Adapter.getObjectContentAsString(data.bucket, data.key)
      result === expected
      there was one(inputStream).close
    }
  }

  "upload" should {
    "return an URI if a aws putObject request succeeded" in new WithAdapter {
      val fileMock = mock[File]
      val putObjectResult = mock[PutObjectResult]

      val result = upload(data.key, fileMock, data.bucket, Map("some" -> "metadata"), PublicS3Visibility)

      result must beSuccessfulTry.withValue(new URI(s"s3://${data.bucket}/${data.key}"))
      there was one(client).putObject(any[PutObjectRequest])
    }
    "throw an exception if the aws putObject request failed" in new WithAdapter {
      val fileMock = mock[File]
      val putObjectResult = mock[PutObjectResult]
      client.putObject(any[PutObjectRequest]) throws new AmazonClientException("damn")

      val result = upload(data.key, fileMock, data.bucket, Map("some" -> "metadata"), PublicS3Visibility)
      result must beFailedTry.withThrowable[AmazonClientException]
    }
  }

  object data {
    val bucket = "some"
    val key = "some/key"
  }

  trait WithAdapter extends AbstractS3Adapter
    with SploggerComponent
    with PropertiesLoaderComponent
    with Scope {

    override val splog = mock[Splogger]
    override lazy val client = mock[AmazonS3Client]
    override val config = mock[PropertiesLoader]

  }

}
