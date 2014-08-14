package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._

abstract class AbstractEmailVerifiedAddressLister extends FulfillmentWorker {
  this: SESAdapterComponent
    with SWFAdapterComponent
    with DynamoAdapterComponent =>

  override def getSpecification: ActivitySpecification = {
    new ActivitySpecification(List())
  }

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

class EmailVerifiedAddressLister(swf: SWFAdapter, dyn: DynamoAdapter, ses: SESAdapter)
  extends AbstractEmailVerifiedAddressLister
  with SWFAdapterComponent
  with DynamoAdapterComponent
  with SESAdapterComponent {
    def swfAdapter = swf
    def dynamoAdapter = dyn
    def sesAdapter = ses
}

object email_verifiedaddresslister {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new EmailVerifiedAddressLister (
      new SWFAdapter(cfg),
      new DynamoAdapter(cfg),
      new SESAdapter(cfg)
    )
     println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}

