package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class EmailSender extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {
  this: SESAdapterComponent =>

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

object email_sender {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new EmailSender
      with SWFAdapterComponent with DynamoAdapterComponent with SESAdapterComponent {
        def swfAdapter = SWFAdapter(cfg)
        def dynamoAdapter = DynamoAdapter(cfg)
        def sesAdapter = SESAdapter(cfg)
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

