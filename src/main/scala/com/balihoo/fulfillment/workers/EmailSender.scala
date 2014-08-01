package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class AbstractEmailSender extends FulfillmentWorker {
  this: SESAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent =>

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      sendEmail(
        params.getRequiredParameter("from"),
        params.getRequiredParameter("recipients").split(",").toList,
        params.getRequiredParameter("subject"),
        params.getRequiredParameter("body"),
        params.getRequiredParameter("type") == "html"
      ).toString
    }
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String, html: Boolean = true): String = {
    sesAdapter.sendEmail(from, recipients, subject, body)
  }
}

class EmailSender(swf: SWFAdapter, dyn: DynamoAdapter, ses: SESAdapter)
  extends AbstractEmailSender
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with SESAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def sesAdapter = ses
}

object email_sender {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new EmailSender (
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new SESAdapter(cfg)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

