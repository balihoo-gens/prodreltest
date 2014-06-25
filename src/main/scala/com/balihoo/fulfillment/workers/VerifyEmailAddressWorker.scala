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
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      verifyAddress(params.getRequiredParameter("address"))
    }
  }

  def verifyAddress(address: String):String = {
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
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

