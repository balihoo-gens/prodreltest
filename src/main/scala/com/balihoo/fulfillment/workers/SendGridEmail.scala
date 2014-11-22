package com.balihoo.fulfillment.workers

import java.io.InputStreamReader
import java.net.URL

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config.PropertiesLoader
import com.balihoo.fulfillment.util.Splogger
import org.joda.time.DateTime
import scala.io.Source

abstract class AbstractSendGridEmail extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SendGridAdapterComponent
  with ScalaCsvAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new StringActivityParameter("sendId", "A string that uniquely identifies this email send for tracking purposes"),
      new StringActivityParameter("subaccount", "The SendGrid subaccount username"),
      new UriActivityParameter("recipientListUrl", "The URL of the recipient list"),
      new StringActivityParameter("subject", "The email subject"),
      new EmailActivityParameter("fromAddress", "The from address"),
      new StringActivityParameter("fromName", "The from name"),
      new EmailActivityParameter("replyToAddress", "The reply-to address"),
      new UriActivityParameter("htmlUrl", "The URL of the file containing the email body"),
      new DateTimeActivityParameter("sendTime", "The desired send time (< 24 hours in the future)")
    ), new StringActivityResult("A success message that makes you feel good, but can be ignored"))
  }

  override def handleTask(params: ActivityParameters) = {
    splog.info(s"Running ${getClass.getSimpleName} handleTask: processing $name")
    withTaskHandling {
      val credentials = sendGridAdapter.getCredentials(params("subaccount"))
      val sendId = params("sendId")
      val subject = params("subject")
      val fromAddress = params("fromAddress")
      val fromName = params("fromName")
      val replyToAddress = params("replyToAddress")
      val body = Source.fromURL(params[String]("htmlUrl")).mkString
      val sendTime = params[DateTime]("sendTime")
      val email = Email(fromAddress = fromAddress, fromName = fromName, replyToAddress = replyToAddress, subject = subject, body = body)
      val recipientReader = new InputStreamReader(new URL(params("recipientListUrl")).openStream())
      val recipientCsv = csvAdapter.parseReaderAsStream(recipientReader)
      sendGridAdapter.sendEmail(credentials, sendId, email, recipientCsv)
      recipientReader.close
      "OK"
    }
  }
}

class SendGridEmail(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractSendGridUpdateSubaccount
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
