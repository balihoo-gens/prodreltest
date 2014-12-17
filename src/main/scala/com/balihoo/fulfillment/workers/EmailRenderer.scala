package com.balihoo.fulfillment.workers

import java.net.{URI, URL}

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import play.api.libs.json._
import scala.util.{Success, Failure, Try}
import java.io.ByteArrayInputStream

class AbstractEmailRenderer extends AbstractRESTClient {
  this: LoggingWorkflowAdapter
  with S3AdapterComponent
  with HTTPAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(
      super.getSpecification.params :+
      new StringParameter("target", "File name for where the body content will be saved"),
      new ObjectResultType("Json object containing the email body and data")
    )
  }

  override def handleTask(params: ActivityArgs) = {
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

    response.code.code match {
      case n if 200 until 300 contains n =>
        // SUCCESS!
        Try(Json.toJson(response.bodyString)) match {
          case Success(jsonResponse) =>
            println(jsonResponse)
            Try((jsonResponse \ "json").as[JsObject]) match {
              case Success(data) =>
                Try((jsonResponse \ "layout").as[String]) match {
                  case Success(body) =>
                    val is = new ByteArrayInputStream(body.getBytes)
                    s3Adapter.uploadStream(s"$target", is, body.size) match {
                      case Success(s3Uri) =>
                        getSpecification.createResult(
                          Json.obj(
                            "body" -> JsString(s3Uri.toString),
                            "data" -> data
                          )
                        )
                      case Failure(t) =>
                        throw new FailTaskException("failed to upload", t.getMessage)
                    }
                  case Failure(t) =>
                    throw new FailTaskException("'layout' tag not found in response", t.getMessage)
                }
              case Failure(t) =>
                throw new FailTaskException("'json' tag not found in response", t.getMessage)
            }
          case Failure(t) =>
            throw new FailTaskException("response not parsable json", t.getMessage)
        }
      case n if 500 until 600 contains n =>
        // Server Error
        throw new CancelTaskException("Server Error", s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
      case _ =>
        // Redirection or Client Error or anything else we didn't anticipate
        throw new Exception(s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
    }
  }
}

class EmailRenderer(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailRenderer
  with LoggingWorkflowAdapterImpl
  with S3AdapterComponent
  with HTTPAdapterComponent {
    private val timeoutSeconds = _cfg.getOrElse("timeoutSeconds", 60)
    lazy private val _http = new HTTPAdapter(timeoutSeconds)
    def httpAdapter = _http
    override val s3Adapter = new S3Adapter(_cfg, _splog)
}

object email_renderer extends FulfillmentWorkerApp {
  override def createWorker(cfg: PropertiesLoader, splog: Splogger): FulfillmentWorker = {
    new EmailRenderer(cfg, splog)
  }
}
