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
import play.api.libs.json._
import scalaj.http._
import java.io._
import com.stackmob.newman.response.{HttpResponse, HttpResponseCode}
import java.net.URL
import scala.util.{Success, Failure, Try}


@RunWith(classOf[JUnitRunner])
class TestEmailRenderer extends Specification with Mockito {

  "AbstractEmailRenderer.handleTask" should {
    "cancel task if the response code is in the 500 range" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      httpAdapterPost returns servErrResponse
      handleTask(params) must throwA[CancelTaskException]
    }
     "fail task if the response code is anything else" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      httpAdapterPost returns unkErrResponse
      handleTask(params) must throwA[FailTaskException]
    }
    "fail task if the response is not json" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      httpAdapterPost returns noJsonResponse
      handleTask(params) must throwA[FailTaskException]
    }
    "fail task if the layout is not a string" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      httpAdapterPost returns badLayoutResponse
      handleTask(params) must throwA[FailTaskException]
    }
    "find a stranger in the alps when the upload fails" in new WithWorker {
      val params = getSpecification.getArgs(data.apGoodReq)
      httpAdapterPost returns goodResponse
      upload returns Failure(new Exception("stranger in the alps"))
      handleTask(params) must throwA[FailTaskException]
    }
     "return the correct S3 Url" in new WithWorker {
      val resultS3Url = s"s3://${data.bucket}/${data.destKey}/${data.fileName}"
      val result = s"""{"body":"$resultS3Url","data":{"thanks":"donny"}}"""
      httpAdapterPost returns goodResponse
      upload returns Success(new URI(resultS3Url))
      val params = getSpecification.getArgs(data.apGoodReq)
      Json.stringify(handleTask(params).result.as[JsObject]) must beEqualTo(result)
    }
  }

  /**
   *Factory method for mock HttpResponse objects
   * @param code the HTTP response code
   * @param body the body of the response
   * @return
   */
  private def buildHttpResponse(code: Int, body: String) = {
    val responseCode = mock[HttpResponseCode]
    responseCode.code returns code

    val response = mock[HttpResponse]
    response.code returns responseCode
    response.bodyString returns body

    response
  }

  val goodResponse = buildHttpResponse(200, """{"json": {"thanks":"donny"}, "layout": "phone's ringing Dude"} """)
  val servErrResponse = buildHttpResponse(500, """{"json": {"thanks":"donny"}, "layout": "phone's ringing Dude"} """)
  val unkErrResponse = buildHttpResponse(300, """{"json": {"thanks":"donny"}, "layout": "phone's ringing Dude"} """)
  val noJsonResponse = buildHttpResponse(200, """{"json": {"thanks":"donny"}, "layout": "phone' """)
  val badLayoutResponse = buildHttpResponse(200, """{"json": {"thanks":"donny"}, "layout": ["phone's", "ringing","Dude"]} """)

  object data {
    val fileName = "some_file.name"

    val apGoodReq = Json.obj(
      "url" -> "http://some/uri",
      "body" -> "stringaling",
      "method" -> "POST",
      "target" -> JsString(fileName)
    )
    val destKey ="some_s3_key"
    val bucket ="some.s3.bucket"
    val fullKey = s"${destKey}/${fileName}"
  }

  trait WithWorker extends AbstractEmailRenderer with Scope
    with LoggingWorkflowAdapterTestImpl
    with HTTPAdapterComponent
    with S3AdapterComponent {

    /* overrides */
    override def destinationS3Key = data.destKey
    val s3Adapter = mock[AbstractS3Adapter]

    /* mocks */
    val mockInputStream = mock[S3ObjectInputStream]
    val httpAdapter = mock[HTTPAdapter]
    def httpAdapterGet = httpAdapter.get(any[URL], any[Seq[(String, Any)]], any[Seq[(String, String)]], any[Option[(String, String)]])
    def httpAdapterPost = httpAdapter.post(any[URL], any[String], any[Seq[(String, Any)]], any[Seq[(String, String)]], any[Option[(String, String)]])
    def upload = s3Adapter.uploadStream(any[String], any[InputStream], any[Int], any[String], any[Map[String, String]], any[S3Visibility])
  }
}
