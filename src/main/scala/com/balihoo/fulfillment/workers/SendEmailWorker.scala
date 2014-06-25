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

  override def handleTask(params: ActivityParameters) = {
    println("SendEmailWorker.handleTask: processing $name")

    try {
      val input:JsObject = Json.parse(task.getInput).as[JsObject]
      val token = task.getTaskToken
      name match {
        case "send-email" =>
          val id = sendEmail(
              params.getRequiredParameter("from"),
              params.getRequiredParameter("recipients").split(",").toList,
              params.getRequiredParameter("subject"),
              params.getRequiredParameter("body"),
              params.getRequiredParameter("type") == "html"
          )
          completeTask(s"""{"$name": "${id.toString}"}""")
       case _ =>
          throw new Exception(s"activity '$name' is NOT IMPLEMENTED")
      }
    } catch {
      case exception:Exception =>
        failTask(s"""{"$name": "${exception.toString}"}""", exception.getMessage)
      case _:Throwable =>
        failTask(s"""{"$name": "Caught a Throwable""", "caught a throwable")
    }
  }

  def sendEmail(from: String, recipients: List[String], subject: String, body: String, html: Boolean = true): String = {
    sesAdapter.sendEmail(from, recipients, subject, body)
  }
}

object sendemailworker {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new SendEmailWorker(new SWFAdapter(config), new SQSAdapter(config), new SESAdapter(config))
    println("Running SendEmailWorker")
    worker.work()
  }
}

