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

  def destinationS3Key = swfAdapter.config.getString("s3dir")

  override def handleTask(args: ActivityArgs) = {
    val url = args[URI]("url").toURL
    val headers = args.getOrElse[Map[String, String]]("headers", Map()).toList
    val method = args[String]("method")
    val target = args[String]("target")
    lazy val body = args[String]("body")
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
        val rs = response.bodyString
        splog.debug(s"Http response: $rs")

        //parse the response as json
        val jsonResponse = Try(Json.parse(rs)) match {
          case Success(jsonResponse) => jsonResponse
          case Failure(t) => throw new FailTaskException(s"response not parsable json: $rs", t.getMessage)
        }

        //extract the json meta data
        val data = Try((jsonResponse \ "json").as[JsValue]) match {
          case Success(data) => data
          case Failure(t) => throw new FailTaskException(s"invalid 'json' tag in response: $rs", t.getMessage)
        }

        //extract the email body, from the 'layout' tag
        val body =  Try((jsonResponse \ "layout").as[String]) match {
          case Success(body) => body
          case Failure(t) => throw new FailTaskException(s"invalid 'layout' tag (must be a string): $rs", t.getMessage)
        }

        //upload the body to s3
        val is = new ByteArrayInputStream(body.getBytes)
        s3Adapter.uploadStream(s"$destinationS3Key/$target", is, body.size) match {
          case Success(s3Uri) =>
            val jsonS3Uri = JsString(s3Uri.toString)
            getSpecification.createResult(Json.obj("body" -> jsonS3Uri, "data" -> data))
          case Failure(t) =>
            throw new FailTaskException("failed to upload", t.getMessage)
        }
      case n if 500 until 600 contains n =>
        // Server Error
        throw new CancelTaskException("Server Error", s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
      case _ =>
        // Redirection or Client Error or anything else we didn't anticipate
        throw new FailTaskException("Unexpected response error", s"Code ${response.code.code} ${response.code.stringVal}: ${response.bodyString}")
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
