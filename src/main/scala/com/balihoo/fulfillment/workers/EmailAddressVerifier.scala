package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters.{DynamoAdapter, SWFAdapter, SESAdapter}
import com.balihoo.fulfillment.config.PropertiesLoader

class EmailAddressVerifier(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter, sesAdapter: SESAdapter)
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

object emailaddressverifier {
  def main(args: Array[String]) {
    val config = PropertiesLoader(args, "email")
    val worker = new EmailAddressVerifier(
      new SWFAdapter(config),
      new DynamoAdapter(config),
      new SESAdapter(config)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

