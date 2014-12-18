package com.balihoo.fulfillment.workers.sendgrid

import java.io.InputStreamReader
import java.net.URI

import com.balihoo.fulfillment.workers._
import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import org.joda.time.DateTime
import play.api.libs.json.JsObject
import resource._

import scala.util.{Failure, Success, Try}

abstract class AbstractSendGridEmail extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SendGridAdapterComponent
  with ScalaCsvAdapterComponent
  with S3AdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ObjectParameter("metadata", "An associative array of values that will appear in the event data"),
      new StringParameter("subaccount", "The SendGrid subaccount username"),
      new UriParameter("listUrl", "The S3 URL of the recipient list"),
      new StringParameter("subject", "The email subject"),
      new EmailParameter("fromAddress", "The from address"),
      new StringParameter("fromName", "The from name"),
      new EmailParameter("replyToAddress", "The reply-to address"),
      new UriParameter("bodyUrl", "The S3 URL of the file containing the email body"),
      new DateTimeParameter("sendTime", "The desired send time (< 24 hours in the future)"),
      new StringParameter("recipientIdHeading", "The heading of the recipientId column"),
      new StringParameter("emailHeading", "The heading of the email column")
    ), new StringResultType("A success message that makes you feel good, but can be ignored"))
  }

  override def handleTask(args: ActivityArgs):ActivityResult = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    val credentials = sendGridAdapter.getCredentials(args("subaccount"))
    val metadata = args[JsObject]("metadata")
    val subject = args[String]("subject")
    val fromAddress = args[String]("fromAddress")
    val fromName = args[String]("fromName")
    val replyToAddress = args[String]("replyToAddress")
    val bodyUrl = args[URI]("bodyUrl")
    val (bodyBucket, bodyKey) = S3Adapter.dissectS3Url(bodyUrl)
    val sendTime = args[DateTime]("sendTime")
    val recipientIdHeading = args[String]("recipientIdHeading")
    val emailHeading = args[String]("emailHeading")
    val listUrl = args[URI]("listUrl")
    val (listBucket, listKey) = S3Adapter.dissectS3Url(listUrl)

    // Retrieve the email body
    val body = Try(s3Adapter.getObjectContentAsString(bodyBucket, bodyKey)) match {
      case Success(s) => s
      case Failure(e) => throw new SendGridException(s"Unable to get email body from $bodyUrl", e)
    }

    // Define the email contents
    val email = Email(fromAddress = fromAddress, fromName = fromName, replyToAddress = replyToAddress, subject = subject, body = body)

    // Create a stream of recipient records and send the emails
    try {
      for (s3Meta <- managed(s3Adapter.getMeta(listBucket, listKey).get);
           reader <- managed(new InputStreamReader(s3Meta.getContentStream))) {
        val recipientCsv = csvAdapter.parseReaderAsStream(reader).get
        processStream(recipientCsv)
        try {
          sendGridAdapter.sendEmail(credentials, metadata, sendTime, email, recipientCsv, recipientIdHeading, emailHeading)
        } catch {
          case e: Exception => throw new SendGridException("Error while sending email", e)
        }
      }
    } catch {
      case e: SendGridException => throw e
      case e: Exception => throw new SendGridException(s"Unable to get recipient list from $listUrl", e)
    }

    getSpecification.createResult("OK")
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
  override val s3Adapter = new S3Adapter(_cfg, _splog)
}

object sendgrid_email extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new SendGridEmail(cfg, splog)
  }
}
