package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class AbstractEmailAddressVerifier extends FulfillmentWorker {
  this: SESAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent =>

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

class EmailAddressVerifier(swf: SWFAdapter, dyn: DynamoAdapter, ses: SESAdapter)
  extends AbstractEmailAddressVerifier
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with SESAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def sesAdapter = ses
}

object email_addressverifier {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))

    val worker = new EmailAddressVerifier(
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new SESAdapter(cfg)
    )

    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

