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
import org.mockito.Matchers._
import play.api.libs.json.{JsArray, JsString, Json}
import scalaj.http._
import java.io._
import scala.Function3

import scala.util.{Success, Try}

@RunWith(classOf[JUnitRunner])
class TestUrlToS3Saver extends Specification with Mockito {

  "AbstractUrlToS3Saver.handleTask" should {
    "fail task if header is invalid" in new WithWorker {
      val params = getSpecification.getArgs(data.apBadHeader)
      handleTask(params) must throwA[IllegalArgumentException]
    }
    "cancel task if the response code is in the 500 range" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      responseKey = "servererr"
      handleTask(params) must throwA[CancelTaskException]
    }
     "fail task if the response code is anything else" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      responseKey = "bad"
      handleTask(params) must throwA[FailTaskException]
    }
     "fail task if the response has no content" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      responseKey = "zerocontent"
      handleTask(params) must throwA[FailTaskException]
    }
    "return the correct S3 Url" in new WithWorker {
      val resultS3Url = s"s3://${data.bucket}/${data.destKey}/${data.fileName}"
      //s3Adapter.uploadStream(s"$destinationS3Key/$target", is, len) match {
      s3Adapter.uploadStream(data.fullKey, mockInputStream, data.contentLength) returns Success(new URI(resultS3Url))
      val params = getSpecification.getArgs(data.apGoodReq)
      responseKey = "good"
      handleTask(params).result.as[JsString].value must beEqualTo(resultS3Url)
    }
  }

  object data {
    val fileName = "some_file.name"

    val baseParams = Json.obj(
      "source" -> "http://some/uri",
      "method" -> "POST",
      "target" -> JsString(fileName)
    )
    val apBadHeader = baseParams + ("headers" -> Json.obj("this is not right" -> Json.arr(1,2,3,4,5)))
    val apGoodReq = baseParams +
      ("headers" -> Json.obj("someheader" -> "stuff")) +
      ("body" -> JsString("stringaling"))
    val destKey ="some_s3_key"
    val bucket ="some.s3.bucket"
    val fullKey = s"${destKey}/${fileName}"
    val contentLength = 100
    val responseData = Map(
      "good" -> (200, Map("Content-Length" -> s"$contentLength")),
      "servererr" -> (500, Map("no" -> "matter")),
      "bad" -> (900, Map("no" -> "matter")),
      "zerocontent" -> (200, Map("Content-Length" -> "0"))
    )
  }

  trait WithWorker extends AbstractUrlToS3Saver with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent {

    var responseKey = "good"

    /* overrides */
    override def destinationS3Key = data.destKey
    override val s3Adapter = mock[AbstractS3Adapter]
    override def executeRequest(req:HttpRequest, callback:(Int, Map[String,String], InputStream) => Unit) = {
      val (code, headers) = data.responseData(responseKey)
      callback(code, headers, mockInputStream)
      new HttpResponse((), code, headers)
    }

    /* mocks */
    val mockInputStream = mock[S3ObjectInputStream]
  }
}
