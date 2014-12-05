package com.balihoo.fulfillment.workers

import java.io.InputStreamReader
import java.net.URI
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import org.joda.time.DateTime
import play.api.libs.json.JsObject
import scala.io.Source
import scala.util.{Try, Success, Failure}
import resource._

abstract class AbstractSendGridEmail extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SendGridAdapterComponent
  with ScalaCsvAdapterComponent
  with S3AdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ObjectActivityParameter("uniqueArgs", "An associative array of values that will appear in the event data"),
      new StringActivityParameter("subaccount", "The SendGrid subaccount username"),
      new UriActivityParameter("listUri", "The S3 URI of the recipient list"),
      new StringActivityParameter("subject", "The email subject"),
      new EmailActivityParameter("fromAddress", "The from address"),
      new StringActivityParameter("fromName", "The from name"),
      new EmailActivityParameter("replyToAddress", "The reply-to address"),
      new UriActivityParameter("bodyUri", "The S3 URI of the file containing the email body"),
      new DateTimeActivityParameter("sendTime", "The desired send time (< 24 hours in the future)"),
      new StringActivityParameter("recipientIdHeading", "The heading of the recipientId column"),
      new StringActivityParameter("emailHeading", "The heading of the email column")
    ), new StringActivityResult("A success message that makes you feel good, but can be ignored"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")
    withTaskHandling {
      val credentials = sendGridAdapter.getCredentials(params("subaccount"))
      val uniqueArgs = params[JsObject]("uniqueArgs")
      val subject = params[String]("subject")
      val fromAddress = params[String]("fromAddress")
      val fromName = params[String]("fromName")
      val replyToAddress = params[String]("replyToAddress")
      val bodyUri = params[URI]("bodyUri")
      val (bodyBucket, bodyKey) = dissectS3Uri(bodyUri, "bodyUri")
      val sendTime = params[DateTime]("sendTime")
      val recipientIdHeading = params[String]("recipientIdHeading")
      val emailHeading = params[String]("emailHeading")
      val listUri = params[URI]("listUri")
      val (listBucket, listKey) = dissectS3Uri(listUri, "listUri")

      // Retrieve the email body
      val body = Try(s3Adapter.getObjectContentAsString(bodyBucket, bodyKey)) match {
        case Success(s) => s
        case Failure(e) => throw new SendGridException(s"Unable to get email body from $bodyUri", e)
      }

      // Define the email contents
      val email = Email(fromAddress = fromAddress, fromName = fromName, replyToAddress = replyToAddress, subject = subject, body = body)

      // Create a stream of recipient records and send the emails
      try {
        for (recipientReader <- managed(s3Adapter.getObjectContentAsReader(listBucket, listKey))) {
          val recipientCsv = csvAdapter.parseReaderAsStream(recipientReader)
          processStream(recipientCsv)
          try {
            sendGridAdapter.sendEmail(credentials, uniqueArgs, sendTime, email, recipientCsv, recipientIdHeading, emailHeading)
          } catch {
            case e: Exception => throw new SendGridException("Error while sending email", e)
          }
        }
      } catch {
        case e: SendGridException => throw e
        case e: Exception => throw new SendGridException(s"Unable to get recipient list from $listUri", e)
      }

      "OK"
    }

    /**
     * Breaks up an S3 URI into useful parts
     * @param uri
     * @param label a label to include in error messages
     * @return the bucket and key
     */
    def dissectS3Uri(uri: URI, label: String): (String, String) = {
      if (uri.getScheme == null || !uri.getScheme.equalsIgnoreCase("s3")) throw new IllegalArgumentException(s"Invalid URI scheme in $label")

      val bucket = uri.getHost
      if (bucket == null || bucket.isEmpty) throw new IllegalArgumentException(s"Missing hostname in $label")

      val path = uri.getPath
      if (path == null || path.isEmpty) throw new IllegalArgumentException(s"Missing path in $label")

      // Strip leading slash from path to get the key
      val key = path.substring(1)

      (bucket, key)
    }
  }

  /**
   * This method should be overridden by the test class so the stream contents can be examined.
   * @param s
   */
  def processStream(s: Stream[Any]): Unit = {}
}

class SendGridEmail(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractSendGridEmail
  with LoggingWorkflowAdapterImpl
  with SendGridAdapterComponent
  with ScalaCsvAdapterComponent
  with S3AdapterComponent {

  private lazy val _sendGridAdapter = new SendGridAdapter(_cfg, _splog)
  def sendGridAdapter = _sendGridAdapter
  override val s3Adapter = new S3Adapter(_cfg)
}

object sendgrid_email extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new SendGridEmail(cfg, splog)
  }
}
