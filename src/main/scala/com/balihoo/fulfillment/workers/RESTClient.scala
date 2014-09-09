package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters.{HTTPAdapter, HTTPAdapterComponent, LoggingWorkflowAdapterImpl, LoggingWorkflowAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger

class AbstractRESTClient extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with HTTPAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("url", "string", "The service URL"),
      new ActivityParameter("method", "string", "DELETE, GET, POST, or PUT"),
      new ActivityParameter("formData", "json", "An object containing the form data for a POST or PUT operations, " +
        "ignored for other operations")
    ), new ActivityResult("json", "An object containing statusCode and body attributes"))
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")
    withTaskHandling {
      //httpAdapter.

      // No exceptions, so call it good.
      "{}"
    }
  }
}

class RESTClient(override val _cfg: PropertiesLoader, override val _splog: Splogger)
    extends AbstractRESTClient
    with LoggingWorkflowAdapterImpl
    with HTTPAdapterComponent {
  val timeoutSeconds = _cfg.getOptInt("timeoutSeconds", 60)
  lazy private val _http = new HTTPAdapter(timeoutSeconds)
  def httpAdapter = _http
}

object rest_client extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new RESTClient(cfg, splog)
  }
}
