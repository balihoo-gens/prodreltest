package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json.{Json, JsObject}
import scala.io.Source
import java.io._
import java.nio.charset.StandardCharsets
import java.net.{URI, URLEncoder}
import scalaj.http._
import java.net.URI
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Success, Failure, Try}
import scala.collection.mutable.{Map => MutableMap}
import play.api.libs.json._

abstract class AbstractUrlToS3Saver extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent =>

  def destinationS3Key = swfAdapter.config.getString("s3dir")
  def taskTimeout = swfAdapter.config.getInt("default_task_start_to_close_timeout")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(
        List(
          new UriActivityParameter("source", "The service URL"),
          new EnumActivityParameter("method", "", List("GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE")),
          new StringActivityParameter("target", "File name for where the body content will be saved"),
          new ObjectActivityParameter("headers", "This object's attributes will be added to the HTTP request headers.", required=false),
          new StringActivityParameter("body", "The request body for POST or PUT operations, ignored for GET and DELETE", required=false)
        ),
        new StringActivityResult("the S3 URL of the saved data.")
      )
  }


  def makeRequest(url:URI, method:String):HttpRequest = Http(url.toString).method(method)

  override def handleTask(params: ActivityParameters) = {
    withTaskHandling {
      val uriPromise = Promise[String]()
      val url = params[URI]("url")
      val method = params[String]("method")
      val target = params[String]("target")
      val req = makeRequest(url, method)

      def processStream(code:Int, headers:Map[String,String], is:InputStream) = {
        code match {
          case n if 200 until 300 contains n =>
            Try(headers("Content-Length").toInt) match {
              case Success(len) => s3Adapter.uploadStream(s"$destinationS3Key/$target", is, len) match {
                case Success(s3Uri) => uriPromise.success(s3Uri.toString)
                case Failure(t) => uriPromise.failure(t)
              }
              case Failure(t) => uriPromise.failure(t)
            }
          case _ => uriPromise.failure(new Exception("server returned $code"))
        }
      }

      if (params.has("headers")) {
        params[JsObject]("headers").validate[Map[String, String]] match {
          case headers:JsSuccess[Map[String, String]] =>
            req.headers(headers.get)
          case _ => throw new IllegalArgumentException("invalid header object")
        }
      }

      if (params.has("body") && (method == "POST")) {
        Try(params[String]("body")) match {
          case Success(body) => req.postData(body)
          case Failure(t) => throw new IllegalArgumentException("invalid body string", t)
        }
      }

      req.exec[Unit](processStream _)
      Await.result(uriPromise.future, taskTimeout seconds)
    }
  }
}

class UrlToS3Saver(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractUrlToS3Saver
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent {
    lazy val _s3Adapter = new S3Adapter(_cfg, _splog)
    def s3Adapter = _s3Adapter
}

object urltos3saver extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new UrlToS3Saver(cfg, splog)
  }
}
