package com.balihoo.fulfillment.adapters

import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3Object
import com.balihoo.fulfillment.config.{PropertiesLoaderComponent, PropertiesLoader}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

@RunWith(classOf[JUnitRunner])
class TestS3AdapterComponent extends Specification with Mockito {
  
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

  class TestContext extends Scope {

    object data {
      val bucket = "hsgks"
      val key = "jdgfksl/fjdskg/fkjds"
    }

    val s3Adapter = new AbstractS3Adapter with SploggerComponent with PropertiesLoaderComponent {
      override val splog = mock[Splogger]
      override lazy val client = mock[AmazonS3Client]
      override val config = mock[PropertiesLoader]
    }

    def givenS3Object() = {
      s3Adapter.client.getObject(data.bucket, data.key) returns mock[S3Object]
    }

    def givenClientThrowsException() = {
      s3Adapter.client.getObject(data.bucket, data.key) throws mock[AmazonClientException]
    }
  }

}
