package com.balihoo.fulfillment.workers

import java.net.URI

import com.balihoo.fulfillment.adapters.{HTTPAdapter, HTTPAdapterComponent, LoggingWorkflowAdapterImpl, LoggingWorkflowAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

class AbstractRESTClient extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with HTTPAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new UriParameter("url", "The service URL"),
      new StringMapParameter("headers", "This object's attributes will be added to the HTTP request headers.", required=false),
      new EnumParameter("method", "", List("DELETE", "GET", "POST", "PUT")),
      new StringParameter("body", "The request body for POST or PUT operations, ignored for GET and DELETE")
    ), new StringResultType("Rest response data"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    val url = args[URI]("url").toURL
    val headers = args.getOrElse[Map[String, String]]("headers", Map()).toList
    val method = args[String]("method")
    lazy val body = args[String]("body")
    splog.info(s"REST client was asked to $method $url")

    val response = method match {
      case "DELETE" => httpAdapter.delete(url, headers = headers)
      case "GET" => httpAdapter.get(url, headers = headers)
      case "POST" => httpAdapter.post(url, body, headers = headers)
      case "PUT" => httpAdapter.put(url, body, headers = headers)
      case _ => throw new IllegalArgumentException(s"Invalid method: $method")
    }

    if(200 <= response.code.code && response.code.code < 300) {
      // SUCCESS!
      getSpecification.createResult(response.bodyString)
    } else if(500 <= response.code.code && response.code.code < 600) {
      // Server Error
      throw new CancelTaskException("Server Error", s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
    } else {
      // Redirection or Client Error or anything else we didn't anticipate
      throw new FailTaskException(s"Code ${response.code.code}", s"${response.code.stringVal}: ${response.bodyString}")
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
