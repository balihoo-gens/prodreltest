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
      new ActivityParameter("url", "string", "The service URL"),
      new ActivityParameter("headers", "JSON", "This object's attributes will be added to the HTTP request headers.", false),
      new ActivityParameter("method", "string", "DELETE, GET, POST, or PUT"),
      new ActivityParameter("body", "string", "The request body for POST or PUT operations, ignored for GET and DELETE")
    ), new ActivityResult("JSON", "An object containing statusCode and body attributes"))
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")
    withTaskHandling {
      val url = new URL(params("url"))
      val headers = Json.parse(params.getOrElse("headers", "{}")).as[Map[String, String]].toList
      val method = params("method")
      lazy val body = params("body")

      val response = method match {
        case "DELETE" => httpAdapter.delete(url, headers)
        case "GET" => httpAdapter.get(url, headers)
        case "POST" => httpAdapter.post(url, body, headers)
        case "PUT" => httpAdapter.put(url, body, headers)
        case _ => throw new IllegalArgumentException(s"Invalid method: $url")
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
    private val timeoutSeconds = _cfg.getOptInt("timeoutSeconds", 60)
    lazy private val _http = new HTTPAdapter(timeoutSeconds)
    def httpAdapter = _http
}

object rest_client extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new RESTClient(cfg, splog)
  }
}
