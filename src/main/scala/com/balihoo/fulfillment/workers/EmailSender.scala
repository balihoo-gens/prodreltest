package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractEmailSender extends FulfillmentWorker {
  this: LoggingWorkflowAdapter
  with SESAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List(
      new EmailParameter("from", ""),
      new StringsParameter("recipients", "Array of email addresses"),
      new StringParameter("subject", ""),
      new StringParameter("body", ""),
      new BooleanParameter("html", "Send email in HTML format")
    ), new ObjectResultType("Result of Send"))
  }

  override def handleTask(params: ActivityArgs):ActivityResult = {
    splog.debug(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    getSpecification.createResult(sendEmail(
      params("from"),
      params[List[String]]("recipients"),
      params("subject"),
      params("body"),
      params[Boolean]("html")
    ).toString)
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
