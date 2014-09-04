package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import com.balihoo.fulfillment.util.Splogger

abstract class AbstractEmailSender extends FulfillmentWorker {
  this: SESAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent =>

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

class EmailSender(swf: SWFAdapter, dyn: DynamoAdapter, ses: SESAdapter, splogger: Splogger)
  extends AbstractEmailSender
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with SESAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def sesAdapter = ses
    def splog = splogger
}

object email_sender {
  def main(args: Array[String]) {
    val name = getClass.getSimpleName.stripSuffix("$")
    val splog = new Splogger(s"/var/log/balihoo/fulfillment/${name}.log")
    splog("INFO", s"Starting $name")
    try {
      val cfg = PropertiesLoader(args, name)
      val worker = new EmailSender (
        new SWFAdapter(cfg),
        new DynamoAdapter(cfg),
        new SESAdapter(cfg)
      )
      worker.work()
    }
    catch {
      case t:Throwable =>
        splog("ERROR", t.getMessage)
    }
  }
}

