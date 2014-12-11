package com.balihoo.fulfillment.workers

import java.net.{URI, URL}

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._


class AbstractEmailWorker extends AbstractRESTClient {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent
  with HTTPAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      super.getSpecification.params :+
      new StringActivityParameter("target", "File name for where the body content will be saved"),
      new ObjectActivityResult("Json object containing the email body and data")
    )
  }

  override def handleTask(params: ActivityParameters) = {
    withTaskHandling {
      val url = params[URI]("url").toURL
      val headers = params.getOrElse("headers", Json.obj()).as[Map[String, String]].toList
      val method = params[String]("method")
      val target = params[String]("target")
      lazy val body = params[String]("body")
      splog.info(s"Email worker was asked to $method $url")

      val response = method match {
        case "DELETE" => httpAdapter.delete(url, headers = headers)
        case "GET" => httpAdapter.get(url, headers = headers)
        case "POST" => httpAdapter.post(url, body, headers = headers)
        case "PUT" => httpAdapter.put(url, body, headers = headers)
        case _ => throw new IllegalArgumentException(s"Invalid method: $method")
      }

      if(200 <= response.code.code && response.code.code < 300) {
        // SUCCESS!
        val jsonResponse = Json.toJson(response.bodyString)
        val data = (jsonResponse \ "json").as[JsObject]
        val body = (jsonResponse \ "layout").as[String]
        val s3Name = s3upload(body, target)
        Json.stringify(Json.obj(
          "body" -> JsString(s3Name),
          "data" -> data
        ))
      } else if(500 <= response.code.code && response.code.code < 600) {
        // Server Error
        throw new CancelTaskException("Server Error", s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
      } else {
        // Redirection or Client Error or anything else we didn't anticipate
        throw new Exception(s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
      }
    }
  }

  def s3upload(body:String, target:String):String = {
    println("uploaded ${body take 12} to ${target}")
    "s3://blablabla"
  }
}

class EmailWorker(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailWorker
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent
  with HTTPAdapterComponent {
    private val timeoutSeconds = _cfg.getOrElse("timeoutSeconds", 60)
    lazy private val _http = new HTTPAdapter(timeoutSeconds)
    def httpAdapter = _http
    override val s3Adapter = new S3Adapter(_cfg, _splog)
}

object email_worker extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new EmailWorker(cfg, splog)
  }
}
