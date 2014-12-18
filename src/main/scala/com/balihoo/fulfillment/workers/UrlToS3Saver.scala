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
          new UriParameter("source", "The service URL"),
          new EnumParameter("method", "", List("GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE")),
          new StringParameter("target", "File name for where the body content will be saved"),
          new StringMapParameter("headers", "This object's attributes will be added to the HTTP request headers.", required=false),
          new StringParameter("body", "The request body for POST or PUT operations, ignored for GET and DELETE", required=false)
        ),
        new StringResultType("the S3 URL of the saved data.")
      )
  }


  def makeRequest(url:URI, method:String):HttpRequest = Http(url.toString).method(method)

  def executeRequest(req:HttpRequest, callback:(Int, Map[String,String], InputStream) => Unit) = {
    req.exec[Unit](callback)
  }

  override def handleTask(args: ActivityArgs) = {
    val uriPromise = Promise[String]()
    val source = args[URI]("source")
    val method = args[String]("method")
    val target = args[String]("target")
    val req = makeRequest(source, method)

    def processStream(code:Int, headers:Map[String,String], is:InputStream) = {
      code match {
        case n if 200 until 300 contains n =>
          Try(headers("Content-Length").toInt) match {
            case Success(len) if len > 0 =>
              s3Adapter.uploadStream(s"$destinationS3Key/$target", is, len) match {
                case Success(s3Uri) =>
                  uriPromise.success(s3Uri.toString)
                case Failure(t) =>
                  uriPromise.failure(new FailTaskException("failed to upload", t.getMessage))
              }
            case Success(len) =>
              uriPromise.failure(new FailTaskException("zero length response", s"content length = $len"))
            case Failure(t) =>
              uriPromise.failure(new FailTaskException("unable to determine response length", t.getMessage))
          }
        case n if 500 until 600 contains n =>
          uriPromise.failure(new CancelTaskException("Server Error", s"server returned $code"))
        case _ =>
          uriPromise.failure(new FailTaskException("Bad Response Code", s"server returned $code"))
      }
    }

    if (args.has("headers")) {
      Try(Json.parse(args[Map[String, String]]("headers")) match {
        case Success(headers) => req.headers(headers)
        case Failure(t) => throw new IllegalArgumentException("invalid header object")
      }
    }

    if (args.has("body") && (method == "POST")) {
      Try(args[String]("body")) match {
        case Success(body) => req.postData(body)
        case Failure(t) => throw new IllegalArgumentException("invalid body string", t)
      }
    }

    executeRequest(req, processStream _)

    getSpecification.createResult(Await.result(uriPromise.future, taskTimeout seconds))
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
