package com.balihoo.fulfillment.workers

import com.balihoo.fulfillment.adapters._
import com.balihoo.fulfillment.config._
import scala.util.Random

abstract class ChaosWorker extends FulfillmentWorker with SWFAdapterComponent with DynamoAdapterComponent {

  override def handleTask(params: ActivityParameters) = {
    val rand = new Random()

    if(rand.nextBoolean()) {
      completeTask("Complete!!!")
    } else if(rand.nextBoolean()) {
      cancelTask("Cancelling!!!")
    } else {
      failTask("Failing!!", "For reasons..")
    }
  }
}

object chaosworker {
  def main(args: Array[String]) {
    val cfg = PropertiesLoader(args, getClass.getSimpleName.stripSuffix("$"))
    val worker = new ChaosWorker
      with SWFAdapterComponent with DynamoAdapterComponent {
        def swfAdapter = SWFAdapter(cfg)
        def dynamoAdapter = DynamoAdapter(cfg)
      }
    println(s"Running ${getClass.getSimpleName}")
    worker.work()
  }
}
