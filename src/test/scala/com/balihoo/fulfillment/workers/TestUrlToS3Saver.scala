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
      val params = getSpecification.getParameters(data.apBadHeader)
      println(params.input)
      handleTask(params) must throwA[IllegalArgumentException]
    }
    /*
    "fail task if the response has no valid Content Length" in new WithWorker {
      handleTask(data.apGoodReq) must throwA[Exception]
    }
    */
    /*
    "return the correct S3 Url" in new WithWorker {
      val resultS3Url = s"s3://${data.bucket}/${data.destKey}/${data.fileName}"
      s3Adapter.uploadStream returns Success(new URI(resultS3Url))
      val params = getSpecification.getParameters(data.apGoodReq)
      handleTask(params) must beEqualTo(resultS3Url)
    }
    */
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
  }

  trait WithWorker extends AbstractUrlToS3Saver with Scope
    with LoggingWorkflowAdapterTestImpl
    with S3AdapterComponent {

    /* overrides */
    override def destinationS3Key = data.destKey
    override val s3Adapter = mock[AbstractS3Adapter]
    /*
    override def makeRequest(url:URI, method:String):HttpRequest = {
      val mockHttp = mock[HttpRequest]
      mockHttp.exec[Unit](any[(Int, Any, InputStream) => Unit]) answers {
      //mockHttp.exec((Int, Map[String,String], InputStream) => Unit) answers {
        f => {
          val func = f.asInstanceOf[Function3[Int, Map[String,String], InputStream, Unit]]
          val resp = func(200, Map("foo" -> "bar"), mockInputStream)
          HttpResponse[Unit]("bla", 200, Map("foo" -> "bar"))
        }
      }
      mockHttp
    }
*/

    /* hack into completeTask base worker to get result */
    var test_complete_result = ""
    override def completeTask(result: String) = {
      test_complete_result = result
    }

    /* mocks */
    val mockInputStream = mock[S3ObjectInputStream]
  }
}
