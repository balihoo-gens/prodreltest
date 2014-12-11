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
import org.scalastuff.json._
import java.net.URI
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Success, Failure, Try}
import scala.collection.mutable.{Map => MutableMap}

abstract class AbstractUrlToS3Saver extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent =>

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
        new UriActivityParameter("url", "The service URL"),
        new ObjectActivityParameter("headers", "This object's attributes will be added to the HTTP request headers.", required=false),
        new EnumActivityParameter("method", "", List("GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE")),
        new StringActivityParameter("body", "The request body for POST or PUT operations, ignored for GET and DELETE", required=false),
        new StringActivityParameter("target", "File name for where the body content will be saved"),
        new StringActivityParameter("jsonpath", "the path inside the response to save", required=false)
      ),
      new ObjectActivityResult("the json wrapped S3 URL of the saved data."))
  }

  override def handleTask(params: ActivityParameters) = {
    withTaskHandling {
      val url = params[URI]("url").toString
      val method = params[String]("method")
      val target = params[String]("target")
      val uriPromise = Promise[String]()

      object handler extends JsonHandler {
        private var saveNext:Boolean = false
        private var _uri = ""
        private var _result = JsObject()

        def s3Uri = _uri

        def startObject() = {}
        def startMember(name: String) = {
          println(name)
          if (name == params[String]("jsonpath")) {
            saveNext = true
            println("found it!")
          }
        }
        def endObject() = {}

        def startArray() = {}
        def endArray() = {}

        def string(s: String) = {
          //this thing doesn't stream the string, which was the whole point.
          //I forked the repo and will work on a version that does at some point, just not now.
          println(s)
          if (saveNext) {
            val bytes = s.getBytes(StandardCharsets.UTF_8)
            val is:InputStream = new ByteArrayInputStream(bytes)
            _uri = s3Adapter.uploadStream(s"urlsaver/$target", is, bytes.size).get.toString
          }
        }
        def number(n: String) = {}
        def trueValue() = {}
        def falseValue() = {}
        def nullValue() = {}

        def error(message: String, line: Int, pos: Int, excerpt: String) = {}
      }

      def processStream(code:Int, headers:Map[String,String], is:InputStream) = {
        val len:Int = headers.getOrElse("Content-Length", throw new Exception("Unable to determine content length")).toInt
        if (params.has("jsonpath")) {
          val parser = new JsonParser(handler)
          parser.parse(new InputStreamReader(is))
          uriPromise.success(handler.s3Uri)
        } else {
          val s3Uri = s3Adapter.uploadStream(s"urlsaver/$target", is, len).get
          uriPromise.success(s3Uri.toString)
        }
      }

      val req:HttpRequest = Http(url).method(method)
      if (params.has("headers")) {
        val headers = params.getOrElse("headers", Json.obj()).as[Map[String, String]]
        req.headers(headers)
      }
      if (params.has("body") && (method == "POST")) {
        req.postData(params[String]("body"))
      }

      req.exec[Unit](processStream _)
      Await.result(uriPromise.future, 30 seconds)

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
