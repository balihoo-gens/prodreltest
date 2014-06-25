package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{
  SQSAdapter,
  SWFAdapter,
  SESAdapter
}
import com.amazonaws.services.simpleworkflow.model.ActivityTask
import com.balihoo.fulfillment.config.PropertiesLoader
import play.api.libs.json._

class ListVerifiedEmailAddressesWorker(swfAdapter: SWFAdapter, sqsAdapter: SQSAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, sqsAdapter) {

  override def handleTask(params: ActivityParameters) = {
    println("ListVerifiedEmailAddressesWorker.handleTask: processing $name")

    try {
      val input:JsObject = Json.parse(task.getInput).as[JsObject]
      val token = task.getTaskToken
      name match {
       case "list-verified-email-addresses" =>
          val result:String = listVerifiedEmailAddresses().mkString(",")
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

  def listVerifiedEmailAddresses(): List[String] = {
    sesAdapter.listVerifiedEmailAddresses()
  }
}

object listverifiedemailaddressworker {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new ListVerifiedEmailAddressesWorker(
      new SWFAdapter(config),
      new SQSAdapter(config),
      new SESAdapter(config)
    )
    println("Running ListVerifiedEmailAddressesWorker")
    worker.work()
  }
}

