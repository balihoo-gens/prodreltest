package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class EmailVerifiedAddressLister extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {
  this: SESAdapterComponent =>

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

object email_verifiedaddresslister {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new EmailVerifiedAddressLister
      with SWFAdapterComponent with DynamoAdapterComponent with SESAdapterComponent {
        def swfAdapter = SWFAdapter(cfg)
        def dynamoAdapter = DynamoAdapter(cfg)
        def sesAdapter = SESAdapter(cfg)
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

