package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters.{DynamoAdapter, SWFAdapter, SESAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader

class VerifyEmailAddressWorker(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

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
      new DynamoAdapter(config),
      new SESAdapter(config)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

