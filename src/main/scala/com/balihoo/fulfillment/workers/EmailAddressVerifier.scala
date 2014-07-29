package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class EmailAddressVerifier extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {
  this: SESAdapterComponent =>

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

object email_addressverifier {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new EmailAddressVerifier
      with SWFAdapterComponent with DynamoAdapterComponent with SESAdapterComponent {
        def swfAdapter = new SWFAdapter with PropertiesLoaderComponent { def config = cfg }
        def dynamoAdapter = new DynamoAdapter with PropertiesLoaderComponent { def config = cfg }
        def sesAdapter = new SESAdapter with PropertiesLoaderComponent { def config = cfg }
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

