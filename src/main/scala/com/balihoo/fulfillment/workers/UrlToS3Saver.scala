package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json.{Json, JsObject}
import scala.io.Source
import java.io._
import java.net.{URI, URLEncoder}
import scalaj.http.Http
import java.net.URI

abstract class AbstractUrlToS3Saver extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent =>

  val s3bucket = swfAdapter.config.getString("s3bucket")

  override def getSpecification: ActivitySpecification = {
      new ActivitySpecification(List(
      new UriActivityParameter("url", "The service URL"),
      new ObjectActivityParameter("headers", "This object's attributes will be added to the HTTP request headers.", false),
      new EnumActivityParameter("method", "", List("DELETE", "GET", "POST", "PUT")),
      new StringActivityParameter("body", "The request body for POST or PUT operations, ignored for GET and DELETE"),
      new StringActivityParameter("target", "File name for where the body content will be saved"),
      new StringActivityParameter("jsonpath", "the path inside the response to save", false)
     ), new ObjectActivityResult("the json wrapped S3 URL of the saved data."))
  }

  override def handleTask(params: ActivityParameters) = {
    withTaskHandling {
      val url = params[URI]("url").toString
      val method = params[String]("method")
      def processStream(is:InputStream) = {
        scala.io.Source.fromInputStream(is).foreach(c => {
          println("stuff" + c))
      }
      Http(url).method(method).execute(parser = processStream)
      "yo"
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
