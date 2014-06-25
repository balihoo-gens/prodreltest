package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{
  SQSAdapter,
  SWFAdapter,
  SESAdapter
}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader
import play.api.libs.json._

class VerifyEmailAddressWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    println("VerifyEmailAddressWorker.handleTask: processing $name")

    try {
      val input:JsObject = Json.parse(task.getInput).as[JsObject]
      val token = task.getTaskToken
      name match {
       case "verify-email-address" =>
          val result:String = verifyAddress(
              params.getRequiredParameter("address")
          )
          completeTask(s"""{"$name": "$result"}""")
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

  def verifyAddress(address: String) = {
    sesAdapter.verifyEmailAddress(address)
  }
}

object verifyemailaddressworker {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new VerifyEmailAddressWorker(
      new SWFAdapter(config),
      new SQSAdapter(config),
      new SESAdapter(config)
    )
    println("Running VerifyEmailAddressWorker")
    worker.work()
  }
}

