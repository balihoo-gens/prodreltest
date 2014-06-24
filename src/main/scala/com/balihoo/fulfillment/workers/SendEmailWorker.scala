package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{
  SQSAdapter,
  SWFAdapter,
  SESAdapter
}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader
import play.api.libs.json._

class SendEmailWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(task: ActivityTask) = {
    println("EmailWorker.handleTask: processing $name")

    try {
      val input:JsObject = Json.parse(task.getInput).as[JsObject]
      val token = task.getTaskToken
      name match {
        case "send-email" =>
          val id = sendEmail(
              getRequiredParameter("from", input, task.getInput),
              getRequiredParameter("recipients", input, task.getInput).split(",").toList,
              getRequiredParameter("subject", input, task.getInput),
              getRequiredParameter("body", input, task.getInput),
              getRequiredParameter("type", input, task.getInput) == "html"
          )
          completeTask(token, s"""{"$name": "${id.toString}"}""")
       case _ =>
          throw new Exception(s"activity '$name' is NOT IMPLEMENTED")
      }
    } catch {
      case exception:Exception =>
        failTask(task.getTaskToken, s"""{"$name": "${exception.toString}"}""", exception.getMessage)
      case _:Throwable =>
        failTask(task.getTaskToken, s"""{"$name": "Caught a Throwable""", "caught a throwable")
    }
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String, html: Boolean = true): String = {
    sesAdapter.sendEmail(from, recipients, subject, body)
  }
}

object sendemailworker {
  def main(args: Array[String]) {
    val config = new PropertiesLoader(".emailworker.properties")
    val worker = new SendEmailWorker(new SWFAdapter(config), new SQSAdapter(config), new SESAdapter(config))
    println("Running SendEmailWorker")
    worker.work()
  }
}

