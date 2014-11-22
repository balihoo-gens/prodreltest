package com.balihoo.fulfillment.workers

import java.net.URL

import com.balihoo.fulfillment.adapters.{HTTPAdapter, HTTPAdapterComponent, LoggingWorkflowAdapterImpl, LoggingWorkflowAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._

class AbstractRESTClient extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with HTTPAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new UriActivityParameter("url", "The service URL"),
      new ObjectActivityParameter("headers", "This object's attributes will be added to the HTTP request headers.", false),
      new EnumActivityParameter("method", "", List("DELETE", "GET", "POST", "PUT")),
      new StringActivityParameter("body", "The request body for POST or PUT operations, ignored for GET and DELETE")
    ), new ObjectActivityResult("An object containing statusCode and body attributes"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")
    withTaskHandling {
      val url = new URL(params("url"))
      val headers = params.getOrElse("headers", Json.obj()).as[Map[String, String]].toList
      val method = params[String]("method")
      lazy val body = params[String]("body")
      splog.info(s"REST client was asked to $method $url")

      val response = method match {
        case "DELETE" => httpAdapter.delete(url, headers = headers)
        case "GET" => httpAdapter.get(url, headers = headers)
        case "POST" => httpAdapter.post(url, body, headers = headers)
        case "PUT" => httpAdapter.put(url, body, headers = headers)
        case _ => throw new IllegalArgumentException(s"Invalid method: $method")
      }

      val responseMap = Map("statusCode" -> response.code.code.toString, "body" -> response.bodyString)
      Json.stringify(Json.toJson(responseMap))
    }
  }
}

class RESTClient(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractRESTClient
  with LoggingWorkflowAdapterImpl
  with HTTPAdapterComponent {
    private val timeoutSeconds = _cfg.getOrElse("timeoutSeconds", 60)
    lazy private val _http = new HTTPAdapter(timeoutSeconds)
    def httpAdapter = _http
}

object rest_client extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new RESTClient(cfg, splog)
  }
}
