package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.{
  DynamoAdapter,
  SWFAdapter,
  SESAdapter
}
import com.balihoo.fulfillment.config.PropertiesLoader

class ListVerifiedEmailAddressesWorker(swfAdapter: SWFAdapter, dynamoAdapter: DynamoAdapter, sesAdapter: SESAdapter)
  extends FulfillmentWorker(swfAdapter, dynamoAdapter) {

  override def handleTask(params: ActivityParameters) = {
    println(s"Running ${getClass.getSimpleName} handleTask: processing $name")

    withTaskHandling {
      listVerifiedEmailAddresses().mkString(",")
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
      new DynamoAdapter(config),
      new SESAdapter(config)
    )
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

