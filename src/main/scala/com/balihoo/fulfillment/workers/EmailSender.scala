package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractEmailSender extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SESAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new ActivityParameter("from", "string", ""),
      new ActivityParameter("recipients", "string", "Comma separated list of email addresses"),
      new ActivityParameter("subject", "string", ""),
      new ActivityParameter("body", "string", ""),
      new ActivityParameter("type", "html|normal", "")
    ), new ActivityResult("JSON", "Result of Send"))
  }

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      sendEmail(
        params("from"),
        params("recipients").split(",").toList,
        params("subject"),
        params("body"),
        params("type") == "html"
      ).toString
    }
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String, html: Boolean = true): String = {
    sesAdapter.sendEmail(from, recipients, subject, body)
  }
}

class EmailSender(override val _cfg: PropertiesLoader, override val _splog: Splogger)
  extends AbstractEmailSender
  with LoggingWorkflowAdapterImpl
  with SESAdapterComponent {
    private lazy val _ses = new SESAdapter(_cfg)
    def sesAdapter = _ses
}

object email_sender extends FulfillmentWorkerApp {
  override def createWorker(cfg:PropertiesLoader, splog:Splogger): FulfillmentWorker = {
    new EmailSender(cfg, splog)
  }
}
