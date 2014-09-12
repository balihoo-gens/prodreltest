package com.balihoo.fulfillment.workers

import java.net.URL

import com.balihoo.fulfillment.adapters.{HTTPAdapter, HTTPAdapterComponent, LoggingWorkflowAdapterImpl, LoggingWorkflowAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.libs.Json

class AbstractRESTClient extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with HTTPAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("url", "string", "The service URL"),
      new ActivityParameter("method", "string", "DELETE, GET, POST, or PUT"),
      new ActivityParameter("body", "string", "The request body for POST or PUT operations, ignored for GET and DELETE")
    ), new ActivityResult("string", "A json object containing statusCode and body attributes"))
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")
    withTaskHandling {
      val url = new URL(params("url"))
      val method = params("method")
      lazy val body = params("body")

      val response = method match {
        case "DELETE" => httpAdapter.delete(url)
        case "GET" => httpAdapter.get(url)
        case "POST" => httpAdapter.post(url, body)
        case "PUT" => httpAdapter.put(url, body)
        case _ => throw new IllegalArgumentException(s"Invalid method: $url")
      }

      Json.stringify(Json.toJson(response))
    }
  }
}

class RESTClient(override val _cfg: PropertiesLoader, override val _splog: Splogger)
    extends AbstractRESTClient
    with LoggingWorkflowAdapterImpl
    with HTTPAdapterComponent {
  lazy private val timeoutSeconds = _cfg.getOptInt("timeoutSeconds", 60)
  lazy private val _http = new HTTPAdapter(timeoutSeconds)
  def httpAdapter = _http
}

object rest_client extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new RESTClient(cfg, splog)
  }
}
