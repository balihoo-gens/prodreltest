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
  with ScalaCsvAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ObjectActivityParameter("uniqueArgs", "An associative array of values that will appear in the event data"),
      new StringActivityParameter("subaccount", "The SendGrid subaccount username"),
      new UriActivityParameter("recipientListUrl", "The URL of the recipient list"),
      new StringActivityParameter("subject", "The email subject"),
      new EmailActivityParameter("fromAddress", "The from address"),
      new StringActivityParameter("fromName", "The from name"),
      new EmailActivityParameter("replyToAddress", "The reply-to address"),
      new UriActivityParameter("htmlUrl", "The URL of the file containing the email body"),
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
      val bodyUri = params[URI]("htmlUrl")
      val body = Try(Source.fromURL(bodyUri.toURL).mkString) match {
        case Success(body) => body
        case Failure(e) => throw new SendGridException(s"Unable to get email body from $bodyUri", e)
      }
      val sendTime = params[DateTime]("sendTime")
      val recipientIdHeading = params[String]("recipientIdHeading")
      val emailHeading = params[String]("emailHeading")
      val email = Email(fromAddress = fromAddress, fromName = fromName, replyToAddress = replyToAddress, subject = subject, body = body)
      val recipientListUri = params[URI]("recipientListUrl")

      try {
        for (recipientReader <- managed(new InputStreamReader(recipientListUri.toURL.openStream()))) {
          val recipientCsv = csvAdapter.parseReaderAsStream(recipientReader)
          sendGridAdapter.sendEmail(credentials, uniqueArgs, sendTime, email, recipientCsv.get, recipientIdHeading, emailHeading)
        }
      } catch {
        case e: Exception => throw new SendGridException(s"Unable to get recipient list from $recipientListUri", e)
      }

      "OK"
    }
  }
}

class SendGridEmail(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractSendGridEmail
  with LoggingWorkflowAdapterImpl
  with SendGridAdapterComponent
  with ScalaCsvAdapterComponent {

  private lazy val _sendGridAdapter = new SendGridAdapter(_cfg, _splog)
  def sendGridAdapter = _sendGridAdapter
}

object sendgrid_email extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new SendGridEmail(cfg, splog)
  }
}
