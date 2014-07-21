package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters.{DynamoAdapter, SWFAdapter, SESAdapter}

import com.balihoo.fulfillment.config.PropertiesLoader

class EmailSender(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

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
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new EmailSender(new SWFAdapter(config), new DynamoAdapter(config), new SESAdapter(config))
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

